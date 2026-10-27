package dejavu

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/siyuan-note/dejavu/cloud"
	"github.com/siyuan-note/dejavu/entity"
	"golang.org/x/sync/singleflight"
)

// CloudSnapshotCache 只保存 S3 面板元数据，零值可用，不写入磁盘或同步仓库。
// 每次查询重验证清单并列举索引版本；来源、凭据、连接配置或仓库密钥变化时隔离缓存。
type CloudSnapshotCache struct {
	mu    sync.Mutex
	scope [32]byte
	state *cloudSnapshotState
}

type cloudSnapshotState struct {
	mu       sync.Mutex
	manifest []*cloud.Index
	etag     string
	loaded   bool
	indexes  map[string]cachedCloudSnapshot
	refs     map[string]cachedCloudSnapshotRef
	refresh  singleflight.Group
	loads    singleflight.Group
}

type cloudSnapshotStamp struct {
	ETag     string
	Size     int64
	Modified time.Time
}

type cachedCloudSnapshot struct {
	stamp cloudSnapshotStamp
	index *entity.Index
}

type cachedCloudSnapshotRef struct {
	stamp cloudSnapshotStamp
	id    string
}

type cloudSnapshotView struct {
	manifest   []*cloud.Index
	objects    map[string]*entity.ObjectInfo
	listingErr error
}

func (cache *CloudSnapshotCache) forRepo(repo *Repo, s3 *cloud.S3) *cloudSnapshotState {
	data, _ := json.Marshal(struct {
		S3   *cloud.ConfS3
		Repo string
		Key  [32]byte
	}{s3.GetConf().S3, repo.Path, sha256.Sum256(repo.store.AesKey)})
	scope := sha256.Sum256(data)
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.state == nil || cache.scope != scope {
		cache.scope = scope
		cache.state = &cloudSnapshotState{indexes: map[string]cachedCloudSnapshot{}, refs: map[string]cachedCloudSnapshotRef{}}
	}
	return cache.state
}

// NewCloudRepoLogLoader 为一次查询固定清单和索引版本，分页按需填充元数据。
// 非 S3 云端或 nil 缓存沿用原有读取；返回值中的日志不与缓存共享可变字段。
func (repo *Repo) NewCloudRepoLogLoader(cache *CloudSnapshotCache) (func(int) ([]*Log, int, int, error), error) {
	s3, ok := repo.cloud.(*cloud.S3)
	if !ok || cache == nil {
		return repo.GetCloudRepoLogs, nil
	}
	state := cache.forRepo(repo, s3)
	value, err, _ := state.refresh.Do("snapshots", func() (interface{}, error) {
		return repo.refreshCloudSnapshotView(s3, state)
	})
	if err != nil {
		repo.observeCloudSnapshotList(err)
		return nil, err
	}
	view := value.(*cloudSnapshotView)
	repo.observeCloudSnapshotList(view.listingErr)
	return func(page int) ([]*Log, int, int, error) {
		if page < 1 {
			page = 1
		}
		total := len(view.manifest)
		pages := (total + 31) / 32
		logs := []*Log{}
		if page > pages {
			return logs, pages, total, nil
		}
		for _, item := range view.manifest[(page-1)*32 : min(page*32, total)] {
			index, readErr := repo.cachedCloudSnapshotIndex(s3, state, item.ID, view.objects[item.ID])
			if errors.Is(readErr, cloud.ErrCloudObjectNotFound) {
				// 清单可能先于索引上传完成；不缓存缺失，下次查询继续读取。
				continue
			}
			if readErr != nil {
				repo.observeCloudSnapshotList(readErr)
				return nil, 0, 0, readErr
			}
			log, readErr := repo.getLog(index, false)
			if readErr != nil {
				return nil, 0, 0, readErr
			}
			logs = append(logs, log)
		}
		return logs, pages, total, nil
	}, nil
}

func (repo *Repo) refreshCloudSnapshotView(s3 *cloud.S3, state *cloudSnapshotState) (*cloudSnapshotView, error) {
	state.mu.Lock()
	etag, loaded, manifest := state.etag, state.loaded, state.manifest
	state.mu.Unlock()
	data, nextETag, unchanged, err := s3.DownloadObjectIfChanged("indexes-v2.json", etag)
	if errors.Is(err, cloud.ErrCloudObjectNotFound) {
		err = nil
		manifest = nil
		nextETag = ""
	} else if err != nil {
		return nil, err
	} else {
		if unchanged && !loaded {
			data, nextETag, unchanged, err = s3.DownloadObjectIfChanged("indexes-v2.json", "")
			if err != nil {
				return nil, err
			}
		}
		if !unchanged {
			decoded, decodeErr := repo.store.compressDecoder.DecodeAll(data, nil)
			if decodeErr != nil {
				return nil, decodeErr
			}
			var parsed *cloud.Indexes
			if err = json.Unmarshal(decoded, &parsed); err != nil {
				return nil, err
			}
			if parsed == nil {
				return nil, errors.New("invalid cloud snapshot list")
			}
			for _, item := range parsed.Indexes {
				if item == nil || !validSnapshotID(item.ID) {
					return nil, errors.New("invalid cloud snapshot list entry")
				}
			}
			manifest = parsed.Indexes
		}
	}
	objects, listErr := s3.ListObjects("indexes/")
	state.mu.Lock()
	state.manifest, state.etag, state.loaded = manifest, usableCloudSnapshotETag(nextETag), true
	if listErr == nil {
		state.pruneIndexes(objects)
	}
	state.mu.Unlock()
	// 列举不可用时仍逐条下载本次清单中的索引，不复用未经版本验证的缓存。
	if listErr != nil {
		objects = nil
	}
	return &cloudSnapshotView{manifest: manifest, objects: objects, listingErr: listErr}, nil
}

func usableCloudSnapshotETag(etag string) string {
	quoted := strings.TrimPrefix(etag, "W/")
	if len(quoted) < 2 || quoted[0] != '"' || quoted[len(quoted)-1] != '"' || strings.ContainsAny(etag, "\r\n") {
		return ""
	}
	for _, ch := range []byte(quoted[1 : len(quoted)-1]) {
		if ch < 0x21 || ch == '"' || ch == 0x7f {
			return ""
		}
	}
	return etag
}

func cloudSnapshotObjectStamp(info *entity.ObjectInfo) cloudSnapshotStamp {
	if info == nil || usableCloudSnapshotETag(info.ETag) == "" {
		return cloudSnapshotStamp{}
	}
	return cloudSnapshotStamp{ETag: info.ETag, Size: info.Size, Modified: info.LastModified}
}

// 调用方持有 state.mu，只回收完整列举中已不存在的对象。
func (state *cloudSnapshotState) pruneIndexes(objects map[string]*entity.ObjectInfo) {
	for id := range state.indexes {
		if objects[id] == nil {
			delete(state.indexes, id)
		}
	}
}

func (repo *Repo) cachedCloudSnapshotIndex(s3 *cloud.S3, state *cloudSnapshotState, id string,
	info *entity.ObjectInfo) (*entity.Index, error) {
	stamp := cloudSnapshotObjectStamp(info)
	// 同一 ID 的元数据可变，缓存只在本次列举的对象版本一致时复用。
	state.mu.Lock()
	cached, exists := state.indexes[id]
	state.mu.Unlock()
	if exists && stamp.ETag != "" && cached.stamp == stamp {
		return cached.index, nil
	}
	flightKey, _ := json.Marshal(struct {
		ID    string
		Stamp cloudSnapshotStamp
	}{id, stamp})
	value, err, _ := state.loads.Do(string(flightKey), func() (interface{}, error) {
		state.mu.Lock()
		cached, exists := state.indexes[id]
		state.mu.Unlock()
		if exists && stamp.ETag != "" && cached.stamp == stamp {
			return cached.index, nil
		}
		data, etag, _, readErr := s3.DownloadObjectIfChanged(path.Join("indexes", id), "")
		if readErr != nil {
			return nil, readErr
		}
		decoded, readErr := repo.store.compressDecoder.DecodeAll(data, nil)
		if readErr != nil {
			return nil, readErr
		}
		index := &entity.Index{}
		if readErr = json.Unmarshal(decoded, index); readErr != nil {
			return nil, readErr
		}
		if index.ID != id {
			return nil, errors.New("cloud index ID mismatch")
		}
		if !index.VerifyAESKey(repo.store.AesKey) {
			return nil, cloud.ErrDecryptFailed
		}
		index.Files = nil
		// 列举后又被覆盖的对象可以用于本次显示，但不能登记为列举时的旧版本。
		if stamp.ETag != "" && etag == stamp.ETag {
			state.mu.Lock()
			state.indexes[id] = cachedCloudSnapshot{stamp: stamp, index: index}
			state.mu.Unlock()
		}
		return index, nil
	})
	if err != nil {
		return nil, err
	}
	return value.(*entity.Index), nil
}

// GetCloudRepoTagLogsCached 每次列举标签和索引版本，只下载新增或变化的引用及索引。
// 不从本地索引复用备注，避免同 ID 的本地修改掩盖云端实际内容；其他云端保持原有行为。
func (repo *Repo) GetCloudRepoTagLogsCached(cache *CloudSnapshotCache, context map[string]interface{}) ([]*Log, error) {
	s3, ok := repo.cloud.(*cloud.S3)
	if !ok || cache == nil {
		return repo.GetCloudRepoTagLogs(context)
	}
	state := cache.forRepo(repo, s3)
	value, err, _ := state.refresh.Do("tags", func() (interface{}, error) {
		return repo.cachedCloudSnapshotTags(s3, state)
	})
	if err != nil {
		repo.observeCloudSnapshotList(err)
		return nil, err
	}
	logs := value.([]*Log)
	ret := make([]*Log, 0, len(logs))
	for _, log := range logs {
		copy := *log
		ret = append(ret, &copy)
	}
	return ret, nil
}

func (repo *Repo) cachedCloudSnapshotTags(s3 *cloud.S3, state *cloudSnapshotState) ([]*Log, error) {
	tags, err := s3.ListObjects("refs/tags/")
	if err != nil {
		return nil, err
	}
	objects, listErr := s3.ListObjects("indexes/")
	repo.observeCloudSnapshotList(listErr)
	if listErr != nil {
		objects = nil
	}
	state.mu.Lock()
	if listErr == nil {
		state.pruneIndexes(objects)
	}
	for name := range state.refs {
		if tags[name] == nil {
			delete(state.refs, name)
		}
	}
	state.mu.Unlock()
	names := make([]string, 0, len(tags))
	for name := range tags {
		names = append(names, name)
	}
	sort.Strings(names)
	logs := []*Log{}
	for _, name := range names {
		info := tags[name]
		stamp := cloudSnapshotObjectStamp(info)
		state.mu.Lock()
		ref, exists := state.refs[name]
		state.mu.Unlock()
		if !exists || stamp.ETag == "" || stamp != ref.stamp {
			data, etag, _, readErr := s3.DownloadObjectIfChanged(path.Join("refs/tags", name), "")
			if readErr != nil {
				return nil, readErr
			}
			id := string(data)
			if !validSnapshotID(id) {
				return nil, errors.New("invalid cloud snapshot tag reference")
			}
			ref = cachedCloudSnapshotRef{stamp: stamp, id: id}
			if stamp.ETag != "" && etag == stamp.ETag {
				state.mu.Lock()
				state.refs[name] = ref
				state.mu.Unlock()
			}
		}
		index, readErr := repo.cachedCloudSnapshotIndex(s3, state, ref.id, objects[ref.id])
		if readErr != nil {
			return nil, readErr
		}
		log, readErr := repo.getLog(index, false)
		if readErr != nil {
			return nil, readErr
		}
		log.Tag = path.Base(name)
		if !info.LastModified.IsZero() {
			log.HTagUpdated = info.LastModified.Format("2006-01-02 15:04:05")
		}
		logs = append(logs, log)
	}
	sort.SliceStable(logs, func(i, j int) bool { return logs[i].Created > logs[j].Created })
	return logs, nil
}

func (repo *Repo) observeCloudSnapshotList(err error) {
	if repo.onCloudIndexList != nil {
		repo.onCloudIndexList(err)
	}
}
