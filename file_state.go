package dejavu

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/filelock"
	"github.com/siyuan-note/logging"
)

type fileStamp struct {
	Size         int64 `json:"size"`
	Modified     int64 `json:"modified"`
	writeVersion uint64
}

var fileWrites sync.Map
var fileWriteSequence atomic.Uint64

// MarkFileChanged 标记本进程已完成的文件写入，使相同大小和时间的已知修改也参与内容校验。
// 调用方应在释放写入锁之前调用；外部保留全部文件属性的修改需要通过完整校验发现。
func MarkFileChanged(path string) {
	fileWrites.Store(filepath.Clean(path), fileWriteSequence.Add(1))
}

type fileObservation struct {
	ID string `json:"id"`
	fileStamp
}

// localFileState 只记录本机已确认内容对应的文件属性，不作为可同步的文件版本。
type localFileState struct {
	Version  int                        `json:"version"`
	IndexID  string                     `json:"indexID"`
	DataPath string                     `json:"dataPath"`
	DeviceID string                     `json:"deviceID"`
	Files    map[string]fileObservation `json:"files"`
}

func stampFile(path string, info os.FileInfo) fileStamp {
	stamp := fileStamp{Size: info.Size(), Modified: info.ModTime().UnixNano()}
	if version, ok := fileWrites.Load(filepath.Clean(path)); ok {
		stamp.writeVersion = version.(uint64)
	}
	return stamp
}

func (repo *Repo) fileStatePath() string {
	return filepath.Join(repo.Path, "file-state-v1.json")
}

func (repo *Repo) newFileState() *localFileState {
	return &localFileState{Version: 1, DataPath: repo.DataPath, DeviceID: repo.DeviceID, Files: map[string]fileObservation{}}
}

// loadFileState 读取本机派生缓存；缓存不可用时从空记录开始，重新核对源文件内容。
func (repo *Repo) loadFileState(indexID string) error {
	state := repo.newFileState()
	data, err := os.ReadFile(repo.fileStatePath())
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		logging.LogWarnf("read local file observations failed, verify file content instead: %s", err)
	}
	if err == nil {
		var saved localFileState
		if json.Unmarshal(data, &saved) == nil && saved.Version == 1 && saved.IndexID == indexID &&
			saved.DataPath == repo.DataPath && saved.DeviceID == repo.DeviceID && saved.Files != nil {
			state = &saved
		} else {
			logging.LogInfof("rebuild local file observations")
		}
	}
	repo.fileStateMu.Lock()
	repo.fileState = state
	repo.fileStateMu.Unlock()
	return nil
}

func (repo *Repo) observedFile(file *entity.File, stamp fileStamp) bool {
	repo.fileStateMu.Lock()
	defer repo.fileStateMu.Unlock()
	if repo.fileState == nil {
		return false
	}
	previous, ok := repo.fileState.Files[file.Path]
	return ok && previous.ID == file.ID && previous.Size == file.Size && previous.fileStamp == stamp
}

func (repo *Repo) rememberFile(file *entity.File, stamp fileStamp) {
	repo.fileStateMu.Lock()
	defer repo.fileStateMu.Unlock()
	if repo.fileState == nil {
		repo.fileState = repo.newFileState()
	}
	repo.fileState.Files[file.Path] = fileObservation{ID: file.ID, fileStamp: stamp}
}

// saveFileState 只发布已验证且属于目标快照的记录；缓存读写失败不影响已认证的快照。
// 缓存未成功保存时保留写入标记，后续操作重新核对内容。
func (repo *Repo) saveFileState(index *entity.Index) error {
	repo.fileStateMu.Lock()
	defer repo.fileStateMu.Unlock()
	if repo.fileState == nil {
		return nil
	}
	files, err := repo.getFiles(index.Files)
	if err != nil {
		return err
	}
	state := repo.newFileState()
	state.IndexID = index.ID
	for _, file := range files {
		if observation, ok := repo.fileState.Files[file.Path]; ok && observation.ID == file.ID && observation.Size == file.Size {
			state.Files[file.Path] = observation
		}
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	previous, readErr := os.ReadFile(repo.fileStatePath())
	if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
		logging.LogWarnf("read local file observations failed, skip cache update: %s", readErr)
		return nil
	}
	if !bytes.Equal(data, previous) {
		if err = writeFileState(repo.fileStatePath(), data); err != nil {
			logging.LogWarnf("save local file observations failed, verify file content next time: %s", err)
			return nil
		}
	}
	for p, observation := range state.Files {
		if observation.writeVersion != 0 {
			fileWrites.CompareAndDelete(filepath.Clean(repo.absPath(p)), observation.writeVersion)
			observation.writeVersion = 0
			state.Files[p] = observation
		}
	}
	repo.fileState = state
	return nil
}

// writeFileState 原子替换派生缓存，并在写入或重命名失败后清理本次临时文件。
func writeFileState(path string, data []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if _, err = file.Write(data); err != nil {
		return err
	}
	if err = file.Sync(); err != nil {
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), path)
}

// matchesObservedContent 在同一文件锁内验证内容及前后属性，防止将读取期间的修改记为已确认。
func (repo *Repo) matchesObservedContent(file *entity.File, stamp fileStamp) (bool, error) {
	abs := repo.absPath(file.Path)
	filelock.Lock(abs)
	defer filelock.Unlock(abs)
	info, err := os.Stat(abs)
	if errors.Is(err, os.ErrNotExist) {
		return false, ErrIndexFileChanged
	}
	if err != nil {
		return false, err
	}
	if stampFile(abs, info) != stamp {
		return false, ErrIndexFileChanged
	}
	matched, err := repo.matchesAssetFile(file)
	if err != nil {
		return false, err
	}
	info, err = os.Stat(abs)
	if errors.Is(err, os.ErrNotExist) {
		return false, ErrIndexFileChanged
	}
	if err != nil {
		return false, err
	}
	if stampFile(abs, info) != stamp {
		return false, ErrIndexFileChanged
	}
	if matched {
		repo.rememberFile(file, stamp)
	}
	return matched, nil
}

func (repo *Repo) diffLocalFiles(files, previous []*entity.File, stamps map[string]fileStamp, fullCheck bool) (upserts, removes []*entity.File, err error) {
	byPath := filesByPath(previous)
	physical := filesByPath(files)
	for i, file := range files {
		if err = repo.checkoutCanceled(); err != nil {
			return
		}
		old := byPath[file.Path]
		if old == nil {
			upserts = append(upserts, file)
			continue
		}
		stamp, onDisk := stamps[file.Path]
		if !onDisk {
			if !equalFile(file, old) {
				upserts = append(upserts, file)
			}
			continue
		}
		if !fullCheck && repo.observedFile(old, stamp) {
			files[i] = old
			continue
		}
		if equalFile(file, old) {
			var same bool
			if same, err = repo.matchesObservedContent(old, stamp); err != nil {
				return
			}
			if same {
				files[i] = old
				continue
			}
		}
		upserts = append(upserts, file)
	}
	for _, file := range previous {
		if physical[file.Path] == nil {
			removes = append(removes, file)
		}
	}
	return
}

func (repo *Repo) diffCheckoutFiles(target, files []*entity.File, stamps map[string]fileStamp) (upserts, removes []*entity.File, err error) {
	byPath := filesByPath(files)
	desired := filesByPath(target)
	for _, file := range target {
		if err = repo.checkoutCanceled(); err != nil {
			return
		}
		if current := byPath[file.Path]; current != nil && equalFile(file, current) {
			var same bool
			if same, err = repo.matchesObservedContent(file, stamps[file.Path]); err != nil {
				return
			}
			if same {
				continue
			}
		}
		upserts = append(upserts, file)
	}
	for _, file := range files {
		if desired[file.Path] == nil {
			removes = append(removes, file)
		}
	}
	return
}
