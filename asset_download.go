package dejavu

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/88250/gulu"
	"github.com/restic/chunker"
	ignore "github.com/sabhiram/go-gitignore"
	"github.com/siyuan-note/dejavu/cloud"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/dejavu/util"
	"github.com/siyuan-note/filelock"
)

var ErrAssetDownloadState = errors.New("invalid asset download state")
var ErrAssetNotDownloaded = errors.New("asset not downloaded")
var ErrAssetApplyPending = errors.New("asset sync recovery required")

// CtxAssetDownloadsAllowed 可由调用方显式禁止本次操作访问远端资源分块。
const CtxAssetDownloadsAllowed = "assetDownloadsAllowed"

// 状态读写使用独立锁，路径解析不等待整个仓库同步过程。
var assetStateFileMu sync.RWMutex

func readAssetStateFile(p string) ([]byte, error) {
	assetStateFileMu.RLock()
	defer assetStateFileMu.RUnlock()
	return os.ReadFile(p)
}

// AssetDownloadStateExists 在同一状态读锁下检查状态与标记，避免文件替换期间误报状态丢失。
func AssetDownloadStateExists(statePath, repoPath string) (bool, error) {
	assetStateFileMu.RLock()
	defer assetStateFileMu.RUnlock()
	_, err := os.Stat(statePath)
	if !errors.Is(err, os.ErrNotExist) {
		return err == nil, err
	}
	_, markerErr := os.Stat(filepath.Join(repoPath, "asset-downloads-v1"))
	if errors.Is(markerErr, os.ErrNotExist) {
		return false, nil
	}
	if markerErr != nil {
		return false, markerErr
	}
	return false, fmt.Errorf("%w: state missing", ErrAssetDownloadState)
}

type assetDownloads struct {
	path     string
	onDemand bool
	state    assetDownloadState
}

type assetDownloadState struct {
	Version  int                     `json:"version"`
	Scope    string                  `json:"scope"`
	Deferred map[string]*entity.File `json:"deferred"`
	Pending  *assetApply             `json:"pending,omitempty"`
	Recovery *assetRecovery          `json:"recovery,omitempty"`
}

// assetApply 在修改工作空间前保存目标和前置版本，恢复时保留额外的本地修改。
type assetApply struct {
	Index    *entity.Index           `json:"index"`
	Base     *entity.Index           `json:"base"`
	Deferred map[string]*entity.File `json:"deferred"`
	Upserts  []*entity.File          `json:"upserts"`
	Removes  []*entity.File          `json:"removes"`
	Before   map[string]*entity.File `json:"before"`
}

// IsAssetDownloadPath 判断可以按需下载的资源实体，辅助元数据始终完整下载。
func IsAssetDownloadPath(p string) bool {
	p = "/" + strings.TrimPrefix(filepath.ToSlash(p), "/")
	if path.Clean(p) != p || !strings.Contains(p, "/assets/") {
		return false
	}
	name := path.Base(p)
	return name != "ocr-texts.json" && !strings.HasSuffix(strings.ToLower(name), ".sya") && !strings.HasPrefix(name, ".")
}

func sameAssetVersion(a, b *entity.File) bool {
	return reflect.DeepEqual(a, b)
}

func validAssetFile(file *entity.File) bool {
	if file == nil || file.Size < 0 || len(file.ID) != 40 || len(file.Chunks) == 0 ||
		!strings.HasPrefix(file.Path, "/") || path.Clean(file.Path) != file.Path || strings.ContainsAny(file.Path, "\\:\x00") {
		return false
	}
	if _, err := hex.DecodeString(file.ID); err != nil {
		return false
	}
	for _, id := range file.Chunks {
		if len(id) != 40 {
			return false
		}
		if _, err := hex.DecodeString(id); err != nil {
			return false
		}
	}
	return true
}

func (repo *Repo) assetStateMarker() string {
	return filepath.Join(repo.Path, "asset-downloads-v1")
}

// ConfigureAssetDownloads 绑定当前设备的持久状态；切换模式不会清除尚未下载的资源。
func (repo *Repo) ConfigureAssetDownloads(onDemand bool, statePath, scope string) error {
	lock.Lock()
	defer lock.Unlock()
	if statePath == "" || scope == "" {
		return ErrAssetDownloadState
	}
	state := assetDownloadState{Version: 1, Scope: scope, Deferred: map[string]*entity.File{}}
	data, err := readAssetStateFile(statePath)
	if err == nil {
		if len(data) < 28 {
			return ErrAssetDownloadState
		}
		data, err = repo.store.decodeData(data)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrAssetDownloadState, err)
		}
		if err = json.Unmarshal(data, &state); err != nil || state.Version != 1 || state.Deferred == nil {
			return ErrAssetDownloadState
		}
		if state.Scope != scope {
			if len(state.Deferred) != 0 || state.Pending != nil || state.Recovery != nil {
				return fmt.Errorf("%w: repository changed", ErrAssetDownloadState)
			}
			files, filesErr := repo.snapshotFiles()
			if filesErr != nil {
				return filesErr
			}
			missing, missingErr := repo.localNotFoundChunks(repo.getChunks(files))
			if missingErr != nil {
				return missingErr
			}
			if len(missing) != 0 {
				return fmt.Errorf("%w: incomplete historical snapshot", ErrAssetDownloadState)
			}
			state.Scope = scope
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	} else if _, markerErr := os.Stat(repo.assetStateMarker()); markerErr == nil || !errors.Is(markerErr, os.ErrNotExist) {
		return fmt.Errorf("%w: state missing", ErrAssetDownloadState)
	} else if !onDemand {
		repo.assetDownloads = nil
		return nil
	}
	for p, f := range state.Deferred {
		if !validAssetFile(f) || p != f.Path || !IsAssetDownloadPath(p) {
			return ErrAssetDownloadState
		}
		stored, getErr := repo.store.GetFile(f.ID)
		if getErr != nil || !sameAssetVersion(stored, f) {
			return fmt.Errorf("%w: resource metadata mismatch", ErrAssetDownloadState)
		}
	}
	repo.assetDownloads = &assetDownloads{path: statePath, onDemand: onDemand, state: state}
	if err = repo.saveAssetState(); err != nil {
		return err
	}
	if err = os.MkdirAll(repo.Path, 0700); err != nil {
		return err
	}
	if err = gulu.File.WriteFileSafer(repo.assetStateMarker(), []byte("1"), 0600); err != nil {
		return err
	}
	return nil
}

// ReadAssetDownloadScope 只读认证后的来源身份，供离线本地操作保持原仓库绑定。
func ReadAssetDownloadScope(statePath string, aesKey []byte) (string, error) {
	data, err := readAssetStateFile(statePath)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if len(data) < 28 {
		return "", ErrAssetDownloadState
	}
	store, err := NewStore("", aesKey)
	if err != nil {
		return "", err
	}
	defer store.compressDecoder.Close()
	defer store.compressEncoder.Close()
	data, err = store.decodeData(data)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrAssetDownloadState, err)
	}
	state := &assetDownloadState{}
	if err = json.Unmarshal(data, state); err != nil || state.Version != 1 || state.Scope == "" || state.Deferred == nil {
		return "", ErrAssetDownloadState
	}
	return state.Scope, nil
}

// ReadDeferredAssets 只读认证后的资源元数据，不获取仓库锁或触发网络、恢复与文件落盘。
func ReadDeferredAssets(statePath string, aesKey []byte) ([]*entity.File, error) {
	data, err := readAssetStateFile(statePath)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) < 28 {
		return nil, ErrAssetDownloadState
	}
	store, err := NewStore("", aesKey)
	if err != nil {
		return nil, err
	}
	defer store.compressDecoder.Close()
	defer store.compressEncoder.Close()
	data, err = store.decodeData(data)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrAssetDownloadState, err)
	}
	state := &assetDownloadState{}
	if err = json.Unmarshal(data, state); err != nil || state.Version != 1 || state.Scope == "" || state.Deferred == nil {
		return nil, ErrAssetDownloadState
	}
	files := state.Deferred
	if state.Pending != nil {
		for p, f := range state.Pending.Deferred {
			files[p] = f
		}
	}
	var ret []*entity.File
	for p, f := range files {
		if !validAssetFile(f) || p != f.Path || !IsAssetDownloadPath(p) {
			return nil, ErrAssetDownloadState
		}
		ret = append(ret, f)
	}
	sort.Slice(ret, func(i, j int) bool { return ret[i].Path < ret[j].Path })
	return ret, nil
}

// HasIncompleteSnapshots 检查保留快照是否缺少内容，不触发网络下载。
func (repo *Repo) HasIncompleteSnapshots() (bool, error) {
	lock.Lock()
	defer lock.Unlock()
	if err := repo.checkAssetState(); err != nil {
		return false, err
	}
	if repo.assetDownloads != nil && repo.assetDownloads.state.Recovery != nil {
		return false, ErrAssetApplyPending
	}
	files, err := repo.snapshotFiles()
	if err != nil {
		return false, err
	}
	missing, err := repo.localNotFoundChunks(repo.getChunks(files))
	return len(missing) != 0, err
}

// EnsureAllSnapshotChunks 补齐所有保留快照，来源切换前必须保留这些历史版本的恢复能力。
func (repo *Repo) EnsureAllSnapshotChunks(context map[string]interface{}) error {
	lock.Lock()
	defer lock.Unlock()
	context, report := repo.assetTrafficContext(context)
	defer report()
	if err := repo.checkAssetState(); err != nil {
		return err
	}
	files, err := repo.snapshotFiles()
	if err != nil {
		return err
	}
	for _, f := range files {
		if err = repo.ensureFileChunks(f, context); err != nil {
			return err
		}
	}
	return nil
}

// ClearAssetDownloadState 仅在当前资源和保留快照均完整时清除状态，用于安全重建仓库或更换密钥。
func (repo *Repo) ClearAssetDownloadState() error {
	lock.Lock()
	defer lock.Unlock()
	if err := repo.checkAssetState(); err != nil {
		return err
	}
	if repo.assetDownloads == nil {
		return nil
	}
	if len(repo.assetDownloads.state.Deferred) != 0 {
		return ErrAssetNotDownloaded
	}
	if repo.assetDownloads.state.Recovery != nil {
		return ErrAssetApplyPending
	}
	files, err := repo.snapshotFiles()
	if err != nil {
		return err
	}
	missing, err := repo.localNotFoundChunks(repo.getChunks(files))
	if err != nil {
		return err
	}
	if len(missing) != 0 {
		return ErrAssetNotDownloaded
	}
	assetStateFileMu.Lock()
	defer assetStateFileMu.Unlock()
	if err = os.Remove(repo.assetStateMarker()); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err = os.Remove(repo.assetDownloads.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	repo.assetDownloads = nil
	return nil
}

func (repo *Repo) snapshotFiles() ([]*entity.File, error) {
	entries, err := os.ReadDir(filepath.Join(repo.Path, "indexes"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var ret []*entity.File
	for _, entry := range entries {
		if entry.IsDir() || len(entry.Name()) != 40 {
			continue
		}
		index, getErr := repo.store.GetIndex(entry.Name())
		if getErr != nil {
			return nil, getErr
		}
		for _, id := range index.Files {
			if seen[id] {
				continue
			}
			seen[id] = true
			file, fileErr := repo.store.GetFile(id)
			if fileErr != nil {
				return nil, fileErr
			}
			ret = append(ret, file)
		}
	}
	return ret, nil
}

func (repo *Repo) saveAssetState() error {
	assetStateFileMu.Lock()
	defer assetStateFileMu.Unlock()
	data, err := json.Marshal(repo.assetDownloads.state)
	if err != nil {
		return err
	}
	if data, err = repo.store.encodeData(data); err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(repo.assetDownloads.path), 0700); err != nil {
		return err
	}
	return gulu.File.WriteFileSafer(repo.assetDownloads.path, data, 0600)
}

func (repo *Repo) checkAssetState() error {
	if err := repo.reloadAssetState(); err != nil {
		return err
	}
	if repo.assetDownloads != nil && repo.assetDownloads.state.Pending != nil {
		return ErrAssetApplyPending
	}
	return nil
}

func (repo *Repo) reloadAssetState() error {
	if repo.assetDownloads != nil {
		data, err := readAssetStateFile(repo.assetDownloads.path)
		if err != nil || len(data) < 28 {
			return fmt.Errorf("%w: state unavailable", ErrAssetDownloadState)
		}
		data, err = repo.store.decodeData(data)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrAssetDownloadState, err)
		}
		state := assetDownloadState{}
		if err = json.Unmarshal(data, &state); err != nil || state.Version != 1 || state.Deferred == nil ||
			state.Scope != repo.assetDownloads.state.Scope {
			return ErrAssetDownloadState
		}
		repo.assetDownloads.state = state
		return nil
	}
	if _, err := os.Stat(repo.assetStateMarker()); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return fmt.Errorf("%w: configuration required", ErrAssetDownloadState)
}

// DeferredAssets 返回逻辑存在但尚未物化的资源版本。
func (repo *Repo) DeferredAssets() ([]*entity.File, error) {
	lock.Lock()
	defer lock.Unlock()
	if err := repo.checkAssetState(); err != nil {
		return nil, err
	}
	var ret []*entity.File
	if repo.assetDownloads != nil {
		for _, f := range repo.assetDownloads.state.Deferred {
			copyFile := *f
			copyFile.Chunks = append([]string{}, f.Chunks...)
			ret = append(ret, &copyFile)
		}
	}
	sort.Slice(ret, func(i, j int) bool { return ret[i].Path < ret[j].Path })
	return ret, nil
}

// EnsureFileChunks 补齐指定历史版本的分块，不覆盖工作空间。
func (repo *Repo) EnsureFileChunks(file *entity.File, context map[string]interface{}) error {
	lock.Lock()
	defer lock.Unlock()
	return repo.ensureFileChunks(file, context)
}

func (repo *Repo) ensureFileChunks(file *entity.File, context map[string]interface{}) error {
	if !validAssetFile(file) {
		return ErrAssetDownloadState
	}
	missing, err := repo.localNotFoundChunks(file.Chunks)
	if err != nil {
		return err
	}
	if len(missing) != 0 {
		if allowed, specified := context[CtxAssetDownloadsAllowed].(bool); specified && !allowed {
			return ErrAssetNotDownloaded
		}
		if repo.cloud == nil {
			return ErrAssetNotDownloaded
		}
		stat, downloadErr := repo.downloadCloudChunksPut(missing, context)
		traffic := &cloud.Traffic{DownloadBytes: stat.CloudBytes, APIGet: stat.CloudCount}
		if total, ok := context[assetTrafficContextKey].(*cloud.Traffic); ok {
			total.DownloadBytes += traffic.DownloadBytes
			total.APIGet += traffic.APIGet
		} else if traffic.APIGet != 0 {
			go repo.cloud.AddTraffic(traffic)
		}
		if err = downloadErr; err != nil {
			return err
		}
	}
	var size int64
	for _, id := range file.Chunks {
		chunk, getErr := repo.store.GetChunk(id)
		if getErr != nil {
			return getErr
		}
		if util.Hash(chunk.Data) != id {
			return fmt.Errorf("resource chunk authentication failed: %s", id)
		}
		size += int64(len(chunk.Data))
	}
	if size != file.Size {
		return fmt.Errorf("resource size mismatch: %s", file.Path)
	}
	return nil
}

// EnsureAsset 获取当前逻辑版本，同一路径的下载和同步由仓库锁串行处理。
func (repo *Repo) EnsureAsset(p string, context map[string]interface{}) (bool, error) {
	lock.Lock()
	defer lock.Unlock()
	if err := repo.checkAssetState(); err != nil {
		return false, err
	}
	return repo.ensureAsset(p, context)
}

func (repo *Repo) ensureAsset(p string, context map[string]interface{}) (bool, error) {
	p = "/" + strings.TrimPrefix(filepath.ToSlash(p), "/")
	if !IsAssetDownloadPath(p) || repo.assetDownloads == nil {
		return false, nil
	}
	f := repo.assetDownloads.state.Deferred[p]
	if f == nil {
		return false, nil
	}
	if err := repo.ensureFileChunks(f, context); err != nil {
		return false, err
	}
	if _, err := os.Stat(repo.absPath(p)); err == nil {
		matches, matchErr := repo.matchesAssetFile(f)
		if matchErr != nil || !matches {
			return false, fmt.Errorf("%w: local resource changed: %s", ErrIndexFileChanged, p)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	} else if err = repo.checkoutFile(f, repo.DataPath, 1, 1, context, nil); err != nil {
		return false, err
	}
	delete(repo.assetDownloads.state.Deferred, p)
	if err := repo.saveAssetState(); err != nil {
		repo.assetDownloads.state.Deferred[p] = f
		return false, err
	}
	return true, nil
}

// EnsureAllAssets 物化当前清单中的全部资源，失败时保留尚未完成的状态。
func (repo *Repo) EnsureAllAssets(context map[string]interface{}) error {
	lock.Lock()
	defer lock.Unlock()
	context, report := repo.assetTrafficContext(context)
	defer report()
	if err := repo.checkAssetState(); err != nil {
		return err
	}
	return repo.ensureAllAssets(context)
}

const assetTrafficContextKey = "assetDownloadTraffic"

// assetTrafficContext 合并批量补齐的云端流量，局域网和本地缓存不计入云端流量
func (repo *Repo) assetTrafficContext(context map[string]interface{}) (map[string]interface{}, func()) {
	ret := make(map[string]interface{}, len(context)+1)
	for key, value := range context {
		ret[key] = value
	}
	traffic := &cloud.Traffic{}
	ret[assetTrafficContextKey] = traffic
	return ret, func() {
		if traffic.APIGet != 0 {
			go repo.cloud.AddTraffic(traffic)
		}
	}
}

// NeedsAssetDownloadsForIndex 检查新忽略规则是否需要从远端补齐资源，不发起网络请求。
func (repo *Repo) NeedsAssetDownloadsForIndex() (bool, error) {
	lock.Lock()
	defer lock.Unlock()
	if err := repo.checkAssetState(); err != nil {
		return false, err
	}
	if repo.assetDownloads == nil {
		return false, nil
	}
	matcher := repo.ignoreMatcher()
	for p, file := range repo.assetDownloads.state.Deferred {
		if !matcher.MatchesPath(p) {
			continue
		}
		missing, err := repo.localNotFoundChunks(file.Chunks)
		if err != nil {
			return false, err
		}
		if len(missing) != 0 {
			return true, nil
		}
	}
	return false, nil
}

func (repo *Repo) ensureAllAssets(context map[string]interface{}) error {
	if repo.assetDownloads == nil {
		return nil
	}
	for p := range repo.assetDownloads.state.Deferred {
		if _, err := repo.ensureAsset(p, context); err != nil {
			return err
		}
	}
	return nil
}

func (repo *Repo) deferredVersion(file *entity.File) bool {
	return repo.assetDownloads != nil && sameAssetVersion(repo.assetDownloads.state.Deferred[file.Path], file)
}

func (repo *Repo) usesAssetDownloads() bool {
	return repo.assetDownloads != nil && (repo.assetDownloads.onDemand || len(repo.assetDownloads.state.Deferred) != 0)
}

func (repo *Repo) shouldDeferAsset(file *entity.File) bool {
	if repo.assetDownloads == nil || !repo.assetDownloads.onDemand || !IsAssetDownloadPath(file.Path) {
		return false
	}
	_, err := os.Stat(repo.absPath(file.Path))
	return errors.Is(err, os.ErrNotExist)
}

// appendDeferredAssets 将未下载版本加入逻辑扫描，实际存在的文件继续按本地变更索引。
func (repo *Repo) appendDeferredAssets(files []*entity.File) ([]*entity.File, error) {
	if repo.assetDownloads == nil {
		return files, nil
	}
	physical := filesByPath(files)
	for p, f := range repo.assetDownloads.state.Deferred {
		if physical[p] == nil {
			files = append(files, f)
		}
	}
	return files, nil
}

func (repo *Repo) promoteIndexedAssets() error {
	if repo.assetDownloads == nil {
		return nil
	}
	removed := map[string]*entity.File{}
	for p, f := range repo.assetDownloads.state.Deferred {
		if _, err := os.Stat(repo.absPath(p)); err == nil {
			removed[p] = f
			delete(repo.assetDownloads.state.Deferred, p)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if len(removed) == 0 {
		return nil
	}
	if err := repo.saveAssetState(); err != nil {
		for p, f := range removed {
			repo.assetDownloads.state.Deferred[p] = f
		}
		return err
	}
	return nil
}

func (repo *Repo) downloadedCloudFiles(files []*entity.File) []*entity.File {
	var ret []*entity.File
	for _, f := range files {
		if !repo.shouldDeferAsset(f) {
			ret = append(ret, f)
		}
	}
	return ret
}

// materializeIgnoredAssets 在停止跟踪资源前保留本地内容和历史分块，下载失败时保留原状态。
func (repo *Repo) materializeIgnoredAssets(matcher *ignore.GitIgnore, context map[string]interface{}) (map[string]bool, error) {
	ignored := map[string]bool{}
	if repo.assetDownloads == nil {
		return ignored, nil
	}
	for p, file := range repo.assetDownloads.state.Deferred {
		if !matcher.MatchesPath(p) {
			continue
		}
		if _, err := os.Stat(repo.absPath(p)); err == nil {
			if err = repo.ensureFileChunks(file, context); err != nil {
				return nil, err
			}
			delete(repo.assetDownloads.state.Deferred, p)
			if err = repo.saveAssetState(); err != nil {
				repo.assetDownloads.state.Deferred[p] = file
				return nil, err
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		} else if _, err = repo.ensureAsset(p, context); err != nil {
			return nil, err
		}
		ignored[p] = true
	}
	return ignored, nil
}

func (repo *Repo) cloudAssetIgnoreMatcher(files []*entity.File, context map[string]interface{}) (*ignore.GitIgnore, error) {
	for _, file := range files {
		if file.Path != "/.siyuan/syncignore" {
			continue
		}
		if err := repo.ensureFileChunks(file, context); err != nil {
			return nil, err
		}
		data, err := repo.openFile(file)
		if err != nil {
			return nil, err
		}
		return ignore.CompileIgnoreLines(strings.Split(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n")...), nil
	}
	return ignore.CompileIgnoreLines(), nil
}

func (repo *Repo) matchesAssetFile(expected *entity.File) (bool, error) {
	if expected == nil {
		return false, nil
	}
	f, err := os.Open(repo.absPath(expected.Path))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.Size() != expected.Size {
		return false, err
	}
	var chunks []string
	if expected.Size < chunker.MinSize {
		data, readErr := io.ReadAll(f)
		if readErr != nil {
			return false, readErr
		}
		chunks = []string{util.Hash(data)}
	} else {
		reader := chunker.NewWithBoundaries(f, repo.chunkPol, chunker.MinSize, chunker.MaxSize)
		buf := make([]byte, chunker.MaxSize)
		for {
			chunk, readErr := reader.Next(buf)
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				return false, readErr
			}
			chunks = append(chunks, util.Hash(chunk.Data))
		}
	}
	return reflect.DeepEqual(chunks, expected.Chunks), nil
}

func (repo *Repo) finishAssetSync(merge *MergeResult, localChanged, publish bool, latest, cloudLatest *entity.Index,
	cloudChunks []string, traffic *TrafficStat, context map[string]interface{}, ignoredAssets map[string]bool) error {
	files, err := repo.getFiles(latest.Files)
	if err != nil {
		return err
	}
	logical := filesByPath(files)
	before := filesByPath(files)
	for _, f := range merge.Removes {
		delete(logical, f.Path)
	}
	for _, f := range merge.Upserts {
		logical[f.Path] = f
	}
	for p := range ignoredAssets {
		delete(logical, p)
		localChanged = true
	}
	target := latest
	if merge.DataChanged() || len(ignoredAssets) != 0 {
		if !localChanged {
			target = cloudLatest
		} else {
			target = &entity.Index{ID: util.RandHash(), Created: time.Now().UnixMilli(), Memo: "[Sync] Cloud sync merge",
				SystemID: repo.DeviceID, SystemName: repo.DeviceName, SystemOS: repo.DeviceOS}
			target.InitAESKeyVerifyVal(repo.store.AesKey)
			for _, f := range logical {
				target.Files = append(target.Files, f.ID)
				target.Size += f.Size
			}
			target.Count = len(target.Files)
		}
	}
	pending := &assetApply{Index: target, Base: cloudLatest, Deferred: map[string]*entity.File{}, Before: map[string]*entity.File{}}
	for p, f := range logical {
		if repo.shouldDeferAsset(f) || (repo.deferredVersion(f) && !repo.assetDownloads.onDemand) {
			pending.Deferred[p] = f
		}
	}
	for _, f := range merge.Upserts {
		if pending.Deferred[f.Path] != nil {
			continue
		}
		if err = repo.ensureFileChunks(f, context); err != nil {
			return err
		}
		pending.Upserts = append(pending.Upserts, f)
		pending.Before[f.Path] = before[f.Path]
	}
	for _, f := range merge.Removes {
		pending.Removes = append(pending.Removes, f)
		pending.Before[f.Path] = before[f.Path]
	}
	if err = repo.store.PutIndex(target); err != nil {
		return err
	}
	if cloudLatest.ID != "" {
		if err = repo.store.PutIndex(cloudLatest); err != nil {
			return err
		}
	}
	repo.assetDownloads.state.Pending = pending
	if err = repo.saveAssetState(); err != nil {
		repo.assetDownloads.state.Pending = nil
		return err
	}
	if err = repo.recoverAssetApply(context); err != nil {
		return err
	}
	if publish && (localChanged || cloudLatest.ID == "") {
		if err = repo.uploadCloud(context, target, cloudLatest, cloudChunks, traffic); err != nil {
			return err
		}
		if err = repo.updateCloudIndexes(target, traffic, context); err != nil {
			return err
		}
	}
	if err = repo.UpdateLatestSync(target); err != nil {
		return err
	}
	if !repo.assetDownloads.onDemand {
		return repo.ensureAllAssets(context)
	}
	return nil
}

func (repo *Repo) recoverAssetApply(context map[string]interface{}) error {
	pending := repo.assetDownloads.state.Pending
	if pending == nil {
		return nil
	}
	if pending.Index == nil || pending.Base == nil || pending.Deferred == nil || pending.Before == nil {
		return ErrAssetDownloadState
	}
	for p, f := range pending.Deferred {
		if !validAssetFile(f) || p != f.Path || !IsAssetDownloadPath(p) {
			return ErrAssetDownloadState
		}
	}
	for _, f := range append(append([]*entity.File{}, pending.Upserts...), pending.Removes...) {
		if !validAssetFile(f) || (pending.Before[f.Path] != nil && !validAssetFile(pending.Before[f.Path])) {
			return ErrAssetDownloadState
		}
	}
	if err := repo.recordAssetRecovery(pending); err != nil {
		return err
	}
	var localChangeErr error
	for _, f := range pending.Upserts {
		matches, err := repo.matchesAssetFile(f)
		if err != nil {
			return err
		}
		if matches {
			continue
		}
		if err = repo.checkAssetBefore(f.Path, pending.Before[f.Path]); err != nil {
			if errors.Is(err, ErrIndexFileChanged) {
				localChangeErr = err
				continue
			}
			return err
		}
		if err = repo.checkoutFile(f, repo.DataPath, 1, 1, context, pending.Before[f.Path]); err != nil {
			if errors.Is(err, ErrIndexFileChanged) {
				localChangeErr = err
				continue
			}
			return err
		}
	}
	for _, f := range pending.Removes {
		if err := func() error {
			abs := repo.absPath(f.Path)
			filelock.Lock(abs)
			defer filelock.Unlock(abs)
			if _, err := os.Stat(abs); errors.Is(err, os.ErrNotExist) {
				return nil
			}
			if err := repo.checkAssetBefore(f.Path, pending.Before[f.Path]); err != nil {
				return err
			}
			return os.Remove(abs)
		}(); err != nil {
			if errors.Is(err, ErrIndexFileChanged) {
				localChangeErr = err
				continue
			}
			return err
		}
	}
	if err := repo.UpdateLatest(pending.Index); err != nil {
		return err
	}
	if pending.Base.ID != "" {
		if err := repo.UpdateLatestSync(pending.Base); err != nil {
			return err
		}
	}
	previous := repo.assetDownloads.state.Deferred
	repo.assetDownloads.state.Deferred = pending.Deferred
	repo.assetDownloads.state.Pending = nil
	if err := repo.saveAssetState(); err != nil {
		repo.assetDownloads.state.Deferred = previous
		repo.assetDownloads.state.Pending = pending
		return err
	}
	return localChangeErr
}

func (repo *Repo) checkAssetBefore(p string, expected *entity.File) error {
	if _, err := os.Stat(repo.absPath(p)); errors.Is(err, os.ErrNotExist) {
		if expected == nil || repo.deferredVersion(expected) {
			return nil
		}
		return fmt.Errorf("%w: file missing: %s", ErrIndexFileChanged, p)
	} else if err != nil {
		return err
	}
	matches, err := repo.matchesAssetFile(expected)
	if err != nil {
		return err
	}
	if !matches {
		return fmt.Errorf("%w: file changed during recovery: %s", ErrIndexFileChanged, p)
	}
	return nil
}
