package dejavu

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/siyuan-note/dejavu/cloud"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/dejavu/util"
)

type appearanceCacheEntry struct {
	key     string
	archive *appearanceArchive
	size    int
}

type appearanceArchiveCache struct {
	mu      sync.Mutex
	entries map[string]*list.Element
	order   *list.List
	size    int
	limit   int
}

func newAppearanceArchiveCache(limit int) *appearanceArchiveCache {
	return &appearanceArchiveCache{entries: map[string]*list.Element{}, order: list.New(), limit: limit}
}

var appearanceMetadataCache = newAppearanceArchiveCache(8 << 20)
var appearancePayloadCache = newAppearanceArchiveCache(64 << 20)

func appearanceCacheChangeTimeStable(changed time.Time) bool {
	return !changed.IsZero() && time.Since(changed) >= time.Second
}

func copyAppearanceArchive(archive *appearanceArchive, payload bool) *appearanceArchive {
	ret := &appearanceArchive{Key: archive.Key, Digest: archive.Digest,
		Parents: append([]string(nil), archive.Parents...), State: append([]byte(nil), archive.State...)}
	if payload {
		ret.Files = make(map[string][]byte, len(archive.Files))
		for p, data := range archive.Files {
			ret.Files[p] = data
		}
	}
	return ret
}

func (cache *appearanceArchiveCache) get(key string) *appearanceArchive {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	entry := cache.entries[key]
	if entry == nil {
		return nil
	}
	cache.order.MoveToFront(entry)
	archive := entry.Value.(*appearanceCacheEntry).archive
	return copyAppearanceArchive(archive, archive.Files != nil)
}

func (cache *appearanceArchiveCache) put(key string, archive *appearanceArchive, payload bool) {
	copy := copyAppearanceArchive(archive, payload)
	size := len(key) + len(copy.State) + len(copy.Parents)*64 + 256
	for p, data := range copy.Files {
		size += len(p) + len(data) + 64
	}
	if size > cache.limit {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if previous := cache.entries[key]; previous != nil {
		cache.size -= previous.Value.(*appearanceCacheEntry).size
		cache.order.Remove(previous)
	}
	cache.entries[key] = cache.order.PushFront(&appearanceCacheEntry{key: key, archive: copy, size: size})
	cache.size += size
	for cache.size > cache.limit {
		entry := cache.order.Back()
		cached := entry.Value.(*appearanceCacheEntry)
		cache.size -= cached.size
		delete(cache.entries, cached.key)
		cache.order.Remove(entry)
	}
}

// appearanceSourceStamp 同时检查文件身份、修改时间及元数据变化时间，不以可保留的 mtime 单独复用校验结果。
func appearanceSourceStamp(paths []string) (string, error) {
	hash := sha256.New()
	for _, p := range paths {
		info, err := os.Lstat(p)
		if err != nil {
			return "", err
		}
		if !info.Mode().IsRegular() {
			return "", fmt.Errorf("appearance source is not a regular file: %s", p)
		}
		stamp, err := appearanceFileChangeStamp(p, info)
		if err != nil || stamp == "" {
			return "", err
		}
		fmt.Fprintf(hash, "%s\x00%s\x00", p, stamp)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// cachedAppearanceArchive 只缓存完整验证过的源；不支持可靠变化时间的文件系统每次重新读取。
func cachedAppearanceArchive(source, key, digest string, paths []string, full bool,
	read func() ([]byte, error)) (*appearanceArchive, error) {
	before, err := appearanceSourceStamp(paths)
	if err != nil {
		return nil, err
	}
	cacheKey := source + "\x00" + key + "\x00" + digest + "\x00" + before
	if before != "" {
		if metadata := appearanceMetadataCache.get(cacheKey); metadata != nil {
			if !full {
				return metadata, nil
			}
			if payload := appearancePayloadCache.get(digest); payload != nil && payload.Key == key {
				return payload, nil
			}
		}
	}
	data, err := read()
	if err != nil {
		return nil, err
	}
	archive, err := decodeAppearanceArchive(key, digest, data)
	if err != nil {
		return nil, err
	}
	after, err := appearanceSourceStamp(paths)
	if err != nil {
		return nil, err
	}
	if before != "" && before != after {
		return nil, fmt.Errorf("%w: appearance source changed during read", ErrIndexFileChanged)
	}
	if before != "" {
		appearanceMetadataCache.put(cacheKey, archive, false)
		appearancePayloadCache.put(digest, archive, true)
	}
	return copyAppearanceArchive(archive, full), nil
}

func (repo *Repo) readAppearanceDiskArchive(key, digest, abs string, full bool) (*appearanceArchive, error) {
	return cachedAppearanceArchive("disk", key, digest, []string{abs}, full, func() ([]byte, error) {
		return os.ReadFile(abs)
	})
}

func (repo *Repo) readAppearanceStoredArchive(file *entity.File, context map[string]interface{}, full bool) (*appearanceArchive, error) {
	key, digest := appearanceArchiveKey(file.Path)
	if !validAssetFile(file) || key == "" {
		return nil, errors.New("invalid appearance archive file")
	}
	missing, err := repo.localNotFoundChunks(file.Chunks)
	if err != nil {
		return nil, err
	}
	if len(missing) != 0 {
		if allowed, specified := context[CtxAssetDownloadsAllowed].(bool); specified && !allowed || repo.cloud == nil {
			return nil, ErrAssetNotDownloaded
		}
		stat, downloadErr := repo.downloadCloudChunksPut(missing, context)
		traffic := &cloud.Traffic{DownloadBytes: stat.CloudBytes, APIGet: stat.CloudCount}
		if total, ok := context[assetTrafficContextKey].(*cloud.Traffic); ok {
			total.DownloadBytes += traffic.DownloadBytes
			total.APIGet += traffic.APIGet
		} else if traffic.APIGet != 0 {
			go repo.cloud.AddTraffic(traffic)
		}
		if downloadErr != nil {
			return nil, downloadErr
		}
	}
	paths := make([]string, 0, len(file.Chunks))
	for _, id := range file.Chunks {
		_, abs := repo.store.AbsPath(id)
		paths = append(paths, abs)
	}
	keyHash := sha256.Sum256(repo.store.AesKey)
	source := fmt.Sprintf("store:%x:%s:%d:%d:%s", keyHash, file.ID, file.Size, file.Updated, strings.Join(file.Chunks, ","))
	return cachedAppearanceArchive(source, key, digest, paths, full, func() ([]byte, error) {
		data := make([]byte, 0, min(file.Size, 64<<20))
		for _, id := range file.Chunks {
			chunk, getErr := repo.store.GetChunk(id)
			if getErr != nil {
				return nil, getErr
			}
			if util.Hash(chunk.Data) != id || int64(len(chunk.Data)) > file.Size-int64(len(data)) {
				return nil, fmt.Errorf("appearance chunk authentication failed: %s", id)
			}
			data = append(data, chunk.Data...)
		}
		if int64(len(data)) != file.Size {
			return nil, errors.New("appearance archive size mismatch")
		}
		return data, nil
	})
}

func hydrateAppearanceHeads(events appearanceEventSet, load func(*appearanceEvent) (*appearanceArchive, error)) error {
	for _, packageEvents := range events {
		for _, head := range appearanceEventHeads(packageEvents) {
			archive, err := load(head)
			if err != nil {
				return err
			}
			head.appearanceArchive = archive
		}
	}
	return nil
}
