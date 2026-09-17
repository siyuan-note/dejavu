package dejavu

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/restic/chunker"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/dejavu/util"
)

// putAppearanceBytes 复用标准分块方式；本地投影只保存内容块，文件元数据仅留在恢复日志中。
func (repo *Repo) putAppearanceBytes(p string, data []byte, persist bool) (*entity.File, error) {
	if path.Clean(p) != p || p == "/" {
		return nil, errors.New("invalid appearance object path")
	}
	if persist {
		if p == appearanceFormatPath {
			if string(data) != appearanceFormatData {
				return nil, errors.New("invalid appearance event format marker")
			}
		} else if key, digest := appearanceArchiveKey(p); key == "" || appearanceArchiveDigest(data) != digest {
			return nil, errors.New("invalid immutable appearance object")
		}
	} else if appearancePackageKey(p) == "" {
		return nil, errors.New("invalid appearance projection path")
	}
	file := entity.NewFile(p, int64(len(data)), appearanceEventModified)
	putChunk := func(content []byte) error {
		id := util.Hash(content)
		if err := repo.store.PutChunk(&entity.Chunk{ID: id, Data: content}); err != nil {
			return err
		}
		file.Chunks = append(file.Chunks, id)
		return nil
	}
	if file.Size < chunker.MinSize {
		if err := putChunk(data); err != nil {
			return nil, err
		}
	} else {
		reader := chunker.NewWithBoundaries(bytes.NewReader(data), repo.chunkPol, chunker.MinSize, chunker.MaxSize)
		buffer := make([]byte, chunker.MaxSize)
		for {
			chunk, err := reader.Next(buffer)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, err
			}
			if err = putChunk(chunk.Data); err != nil {
				return nil, err
			}
		}
	}
	if persist {
		previous, err := repo.store.GetFile(file.ID)
		if err == nil {
			if !reflect.DeepEqual(previous, file) {
				return nil, fmt.Errorf("immutable appearance object metadata changed: %s", p)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		// 元数据缓存跨仓库共享，命中缓存不能替代当前仓库中的持久对象。
		if err = repo.store.PutFile(file); err != nil {
			return nil, err
		}
	}
	return file, nil
}

// appearanceProjectionFiles 将完整归档转换成日志内的投影文件，旧快照的直接文件继续保留。
func (repo *Repo) appearanceProjectionFiles(files []*entity.File, context map[string]interface{}) (map[string]*entity.File, error) {
	ret := map[string]*entity.File{}
	if !repo.appearanceSyncEnabled || repo.appearanceProtocolIgnored() {
		return ret, nil
	}
	var archives []*entity.File
	for _, file := range files {
		if file == nil {
			return nil, errors.New("missing appearance projection input file")
		}
		if key, _ := appearanceArchiveKey(file.Path); key != "" {
			if !repo.appearanceIgnored(key) {
				archives = append(archives, file)
			}
		} else if key := appearancePackageKey(file.Path); key != "" && !repo.appearanceIgnored(key) {
			ret[file.Path] = file
		}
	}
	events, err := repo.readAppearanceEvents(archives, context)
	if err != nil {
		return nil, err
	}
	if events, err = unionAppearanceEvents(events); err != nil {
		return nil, err
	}
	for key, packageEvents := range events {
		selected := selectAppearanceEvent(packageEvents, nil)
		if selected == nil {
			return nil, fmt.Errorf("appearance event has no head: %s", key)
		}
		for p := range ret {
			if appearancePackageKey(p) == key {
				delete(ret, p)
			}
		}
		projection, err := repo.appearanceArchiveProjectionFiles(selected.appearanceArchive)
		if err != nil {
			return nil, err
		}
		for p, file := range projection {
			ret[p] = file
		}
	}
	return ret, nil
}

// appearanceArchiveProjectionFiles 为已选定的完整事件生成恢复日志或冲突历史所需的虚拟文件。
func (repo *Repo) appearanceArchiveProjectionFiles(archive *appearanceArchive) (map[string]*entity.File, error) {
	if archive == nil || !validAppearanceArchiveKey(archive.Key) {
		return nil, errors.New("invalid appearance projection archive")
	}
	if err := validateAppearanceArchiveFiles(archive.State, archive.Files); err != nil {
		return nil, err
	}
	ret := map[string]*entity.File{}
	statePath := appearanceRecordPath(archive.Key)
	file, err := repo.putAppearanceBytes(statePath, archive.State, false)
	if err != nil {
		return nil, err
	}
	ret[statePath] = file
	for relative, content := range archive.Files {
		p := archive.Key + "/" + relative
		file, err = repo.putAppearanceBytes(p, content, false)
		if err != nil {
			return nil, err
		}
		ret[p] = file
	}
	return ret, nil
}

// appearanceBeforeProjectionFiles 仅使用磁盘精确匹配的事件头作为发布前版本，不接纳祖先回滚或任意外部编辑。
func (repo *Repo) appearanceBeforeProjectionFiles(files []*entity.File, context map[string]interface{}) (map[string]*entity.File, error) {
	ret, err := repo.appearanceProjectionFiles(files, context)
	if err != nil || !repo.appearanceSyncEnabled || repo.appearanceProtocolIgnored() {
		return ret, err
	}
	var archives []*entity.File
	for _, file := range files {
		if key, _ := appearanceArchiveKey(file.Path); key != "" && !repo.appearanceIgnored(key) {
			archives = append(archives, file)
		}
	}
	events, err := repo.readAppearanceEvents(archives, context)
	if err != nil {
		return nil, err
	}
	if events, err = unionAppearanceEvents(events); err != nil {
		return nil, err
	}
	for key, packageEvents := range events {
		current, readErr := repo.readAppearanceProjectionForCheck(key)
		if errors.Is(readErr, os.ErrNotExist) {
			continue
		}
		if readErr != nil {
			return nil, readErr
		}
		for _, head := range appearanceEventHeads(packageEvents) {
			if !sameAppearanceProjection(current, head.appearanceArchive) {
				continue
			}
			projection, err := repo.appearanceArchiveProjectionFiles(head.appearanceArchive)
			if err != nil {
				return nil, err
			}
			for p := range ret {
				if appearancePackageKey(p) == key {
					delete(ret, p)
				}
			}
			for p, file := range projection {
				ret[p] = file
			}
			break
		}
	}
	return ret, nil
}

// appearanceProjectionChanged 只比较本机归档、安装状态和实际内容，不修复状态或向仓库写入对象。
func (repo *Repo) appearanceProjectionChanged() (bool, error) {
	if !repo.appearanceSyncEnabled || repo.appearanceProtocolIgnored() {
		return false, nil
	}
	if repo.appearanceUserMatcher().MatchesPath(appearanceFormatPath) {
		return false, errors.New("appearance event format marker is individually ignored")
	}
	events, err := repo.readAppearanceDiskEvents()
	if err != nil {
		return false, err
	}
	keys := map[string]bool{}
	for key, packageEvents := range events {
		if repo.appearanceIgnored(key) {
			continue
		}
		keys[key] = true
		for _, event := range packageEvents {
			if repo.appearanceUserMatcher().MatchesPath(appearanceArchivePath(event.appearanceArchive)) {
				return false, fmt.Errorf("appearance event is individually ignored: %s", event.Digest)
			}
		}
	}
	for _, kind := range []string{"themes", "icons"} {
		entries, readErr := os.ReadDir(repo.absPath("/storage/bazaar/" + kind))
		if errors.Is(readErr, os.ErrNotExist) {
			continue
		}
		if readErr != nil {
			return false, readErr
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
				continue
			}
			key := "/" + kind + "/" + strings.TrimSuffix(entry.Name(), ".json")
			if repo.appearanceIgnored(key) {
				continue
			}
			if !validAppearanceArchiveKey(key) {
				return false, fmt.Errorf("invalid appearance projection key: %s", key)
			}
			keys[key] = true
		}
	}
	changed := false
	for key := range keys {
		if repo.appearanceUserMatcher().MatchesPath(appearanceRecordPath(key)) {
			return false, fmt.Errorf("appearance package record is ignored: %s", key)
		}
		projection, readErr := repo.readAppearanceProjectionForCheck(key)
		if errors.Is(readErr, os.ErrNotExist) {
			changed = true
			continue
		}
		if readErr != nil {
			return false, readErr
		}
		selected := selectAppearanceEvent(events[key], nil)
		if selected == nil || !sameAppearanceProjection(projection, selected.appearanceArchive) {
			changed = true
		}
	}
	return changed, nil
}

func (repo *Repo) readAppearanceProjectionForCheck(key string) (*appearanceArchive, error) {
	statePath := repo.absPath(appearanceRecordPath(key))
	info, err := os.Lstat(statePath)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("appearance state is not a regular file: %s", key)
	}
	state, err := os.ReadFile(statePath)
	if err != nil {
		return nil, err
	}
	if err = validateAppearanceArchiveJSON(state); err != nil {
		return nil, err
	}
	var record appearanceRecord
	if err = json.Unmarshal(state, &record); err != nil {
		return nil, err
	}
	if record.Version != 1 || record.Files == nil || (record.Deleted && len(record.Files) != 0) ||
		(!record.Deleted && len(record.Files) == 0) {
		return nil, fmt.Errorf("unsupported or invalid appearance state: %s", key)
	}
	if err = validateAppearanceArchivePaths(record.Files); err != nil {
		return nil, err
	}
	ret := &appearanceArchive{Key: key, State: state, Files: map[string][]byte{}}
	root := repo.absPath(key)
	err = filepath.WalkDir(root, func(abs string, entry fs.DirEntry, walkErr error) error {
		if errors.Is(walkErr, os.ErrNotExist) && abs == root {
			return nil
		}
		if walkErr != nil {
			return walkErr
		}
		relative, relErr := filepath.Rel(root, abs)
		if relErr != nil {
			return relErr
		}
		relative = filepath.ToSlash(relative)
		if relative != "." && ignoredAppearanceRelative(relative, entry.IsDir()) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("appearance projection symlink is not supported: %s", abs)
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("appearance projection is not a regular file: %s", abs)
		}
		if !validAppearanceArchiveRelative(relative) {
			return fmt.Errorf("invalid appearance projection path: %s", relative)
		}
		if repo.appearanceUserMatcher().MatchesPath(key + "/" + relative) {
			return fmt.Errorf("appearance package is partially ignored: %s/%s", key, relative)
		}
		content, readErr := os.ReadFile(abs)
		if readErr != nil {
			return readErr
		}
		ret.Files[relative] = content
		return nil
	})
	if err != nil {
		return nil, err
	}
	actualPaths := map[string]string{}
	for relative, content := range ret.Files {
		actualPaths[relative] = appearanceArchiveDigest(content)
	}
	if err = validateAppearanceArchivePaths(actualPaths); err != nil {
		return nil, err
	}
	return ret, nil
}
