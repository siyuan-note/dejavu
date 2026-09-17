package dejavu

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/88250/gulu"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/filelock"
	"github.com/siyuan-note/logging"
)

func (repo *Repo) lockAppearance() func() {
	key := filepath.Join(repo.DataPath, ".siyuan-appearance")
	filelock.Lock(key)
	return func() { filelock.Unlock(key) }
}

func (repo *Repo) withAppearanceLock(operation func() error) error {
	defer repo.lockAppearance()()
	return operation()
}

func (repo *Repo) checkoutAppearancePackages(targetFiles, upserts, removes []*entity.File, context map[string]interface{}) error {
	changed := append(append([]*entity.File{}, upserts...), removes...)
	if err := repo.ensureAppearanceJournal(changed); err != nil {
		return err
	}
	var packageUpserts, packageRemoves []*entity.File
	for _, file := range upserts {
		if appearancePackageKey(file.Path) != "" {
			packageUpserts = append(packageUpserts, file)
		}
	}
	for _, file := range removes {
		if appearancePackageKey(file.Path) != "" {
			packageRemoves = append(packageRemoves, file)
		}
	}
	if len(packageUpserts)+len(packageRemoves) == 0 {
		return nil
	}
	latest, err := repo.Latest()
	if err != nil {
		return err
	}
	beforeFiles, err := repo.getFiles(latest.Files)
	if err != nil {
		return err
	}
	before := filesByPath(beforeFiles)
	pending := &assetApply{Index: latest, Base: &entity.Index{}, Upserts: packageUpserts, Removes: packageRemoves,
		Before: map[string]*entity.File{}, Deferred: map[string]*entity.File{}}
	for p, file := range repo.assetDownloads.state.Deferred {
		pending.Deferred[p] = file
	}
	if err = repo.completeAppearanceApply(pending, filesByPath(targetFiles), before, context); err != nil {
		return err
	}
	if err = repo.preflightAppearanceApply(pending); err != nil {
		return err
	}
	repo.assetDownloads.state.Pending = pending
	if err = repo.saveAssetState(); err != nil {
		repo.assetDownloads.state.Pending = nil
		return err
	}
	return repo.recoverAssetApply(context)
}

func withoutAppearanceFiles(files []*entity.File) []*entity.File {
	var ret []*entity.File
	for _, file := range files {
		if appearancePackageKey(file.Path) == "" {
			ret = append(ret, file)
		}
	}
	return ret
}

func (repo *Repo) materializeDeferredAppearance(context map[string]interface{}) error {
	if !repo.appearanceSyncEnabled || repo.assetDownloads == nil {
		return nil
	}
	var missing []*entity.File
	for p, file := range repo.assetDownloads.state.Deferred {
		if appearancePackageKey(p) != "" {
			missing = append(missing, file)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	latest, err := repo.Latest()
	if err != nil {
		return err
	}
	files, err := repo.getFiles(latest.Files)
	if err != nil {
		return err
	}
	if _, err = repo.validateAppearancePackages(files, context); err != nil {
		return err
	}
	logical := filesByPath(files)
	pending := &assetApply{Index: latest, Base: repo.latestSync(), Upserts: missing,
		Before: map[string]*entity.File{}, Deferred: map[string]*entity.File{}}
	for p, file := range repo.assetDownloads.state.Deferred {
		pending.Deferred[p] = file
	}
	if err = repo.completeAppearanceApply(pending, logical, logical, context); err != nil {
		return err
	}
	if err = repo.preflightAppearanceApply(pending); err != nil {
		return err
	}
	repo.assetDownloads.state.Pending = pending
	if err = repo.saveAssetState(); err != nil {
		repo.assetDownloads.state.Pending = nil
		return err
	}
	return repo.recoverAssetApply(context)
}

// ensureAppearanceJournal 在首次处理外观包时启用既有恢复格式，普通文件同步不创建额外状态。
func (repo *Repo) ensureAppearanceJournal(files []*entity.File) error {
	if !repo.appearanceSyncEnabled {
		return nil
	}
	for _, file := range files {
		archiveKey, _ := appearanceArchiveKey(file.Path)
		if appearancePackageKey(file.Path) == "" && (!repo.appearanceSyncEnabled ||
			(archiveKey == "" && file.Path != appearanceFormatPath)) {
			continue
		}
		repo.appearanceJournal = true
		if repo.assetDownloads != nil {
			return nil
		}
		config := repo.assetDownloadConfig
		if config == nil {
			return ErrAssetDownloadState
		}
		repo.assetDownloads = &assetDownloads{path: config.path, state: assetDownloadState{
			Version: 1, Scope: config.scope, Deferred: map[string]*entity.File{},
		}}
		if err := repo.saveAssetState(); err != nil {
			repo.assetDownloads = nil
			return err
		}
		if err := os.MkdirAll(repo.Path, 0700); err != nil {
			return err
		}
		return gulu.File.WriteFileSafer(repo.assetStateMarker(), []byte("1"), 0600)
	}
	return nil
}

// completeAppearanceApply 保存完整目标与前置版本，使恢复不依赖已经被替换的工作空间文件。
func (repo *Repo) completeAppearanceApply(pending *assetApply, target, before map[string]*entity.File, context map[string]interface{}) error {
	keys := map[string]bool{}
	for _, file := range append(append([]*entity.File{}, pending.Upserts...), pending.Removes...) {
		if key := appearancePackageKey(file.Path); key != "" {
			keys[key] = true
		}
	}
	for i, file := range pending.Removes {
		if appearancePackageKey(file.Path) == "" {
			continue
		}
		if previous := before[file.Path]; previous != nil {
			pending.Removes[i] = previous
		} else {
			return fmt.Errorf("%w: unindexed appearance package file: %s", ErrIndexFileChanged, file.Path)
		}
	}
	for p, file := range target {
		if !keys[appearancePackageKey(p)] {
			continue
		}
		if err := repo.ensureFileChunks(file, context); err != nil {
			return err
		}
		pending.Upserts = appendUniqueSyncFile(pending.Upserts, file)
		pending.Before[p] = before[p]
		delete(pending.Deferred, p)
	}
	for p, file := range before {
		if !keys[appearancePackageKey(p)] {
			continue
		}
		pending.Before[p] = file
		if target[p] == nil {
			pending.Removes = appendUniqueSyncFile(pending.Removes, file)
		}
	}
	return nil
}

// preflightAppearanceApply 在持久化恢复记录前发现索引后的本地安装，允许下一轮重新索引。
func (repo *Repo) preflightAppearanceApply(pending *assetApply) error {
	keys := map[string]bool{}
	targets := map[string]*entity.File{}
	for _, file := range pending.Upserts {
		if key := appearancePackageKey(file.Path); key != "" {
			keys[key] = true
			targets[file.Path] = file
		}
	}
	for _, file := range pending.Removes {
		if key := appearancePackageKey(file.Path); key != "" {
			keys[key] = true
		}
	}
	for key := range keys {
		live := repo.absPath(key)
		if err := repo.checkAppearanceParents(live); err != nil {
			return err
		}
		if err := repo.checkAppearanceDirectory(live, key, targets, pending.Before, false); err != nil {
			return err
		}
		for p, file := range pending.Before {
			if file == nil || !strings.HasPrefix(p, key+"/") || repo.deferredVersion(file) {
				continue
			}
			if _, err := os.Stat(repo.absPath(p)); errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("%w: appearance package file missing before apply: %s", ErrIndexFileChanged, p)
			} else if err != nil {
				return err
			}
		}
		p := appearanceRecordPath(key)
		if err := repo.checkAppearanceParents(repo.absPath(p)); err != nil {
			return err
		}
		if err := repo.checkAssetBefore(p, pending.Before[p]); err != nil {
			if target := targets[p]; target != nil {
				if matches, matchErr := repo.matchesAssetFile(target); matchErr == nil && matches {
					continue
				}
			}
			return err
		}
	}
	return nil
}

func (repo *Repo) recoverAppearanceApply(pending *assetApply, context map[string]interface{}) error {
	keys := map[string]bool{}
	targets := map[string]*entity.File{}
	for _, file := range pending.Upserts {
		if key := appearancePackageKey(file.Path); key != "" {
			keys[key] = true
			targets[file.Path] = file
		}
	}
	for _, file := range pending.Removes {
		if key := appearancePackageKey(file.Path); key != "" {
			keys[key] = true
		}
	}
	if len(keys) == 0 {
		return nil
	}
	// 旧版恢复记录只保存差异，未变化的包文件从目标快照补齐。
	indexFiles, err := repo.getFiles(pending.Index.Files)
	if err != nil {
		return err
	}
	removed := filesByPath(pending.Removes)
	for _, file := range indexFiles {
		if keys[appearancePackageKey(file.Path)] && targets[file.Path] == nil && removed[file.Path] == nil {
			targets[file.Path] = file
			if _, exists := pending.Before[file.Path]; !exists {
				pending.Before[file.Path] = file
			}
		}
	}
	packages, err := repo.validateAppearancePackages(mapAppearanceFiles(targets), context)
	if err != nil {
		return err
	}
	ordered := make([]string, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	for _, key := range ordered {
		pkg := packages[key]
		if pkg == nil {
			pkg = &appearancePackage{Key: key, Files: map[string]*entity.File{}}
		}
		if err = repo.applyAppearancePackage(pending, pkg, context); err != nil {
			return err
		}
	}
	return nil
}

func mapAppearanceFiles(files map[string]*entity.File) []*entity.File {
	ret := make([]*entity.File, 0, len(files))
	for _, file := range files {
		ret = append(ret, file)
	}
	return ret
}

func (repo *Repo) appearanceTransactionPaths(id, key string) (live, stage, backup string, err error) {
	if len(id) != 40 || appearancePackageKey(key+"/file") != key {
		return "", "", "", ErrAssetDownloadState
	}
	for _, c := range id {
		if !strings.ContainsRune("0123456789abcdef", c) {
			return "", "", "", ErrAssetDownloadState
		}
	}
	live = repo.absPath(key)
	parent := filepath.Dir(live)
	stem := ".siyuan-appearance-" + filepath.Base(live) + "-" + id
	return live, filepath.Join(parent, stem+"-new"), filepath.Join(parent, stem+"-old"), nil
}

func (repo *Repo) applyAppearancePackage(pending *assetApply, pkg *appearancePackage, context map[string]interface{}) error {
	live, stage, backup, err := repo.appearanceTransactionPaths(pending.Index.ID, pkg.Key)
	if err != nil {
		return err
	}
	if err = repo.checkAppearanceParents(live); err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(live), 0755); err != nil {
		return err
	}
	for _, candidate := range []string{live, stage, backup} {
		if info, statErr := os.Lstat(candidate); statErr == nil {
			if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("invalid appearance package directory: %s", candidate)
			}
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return statErr
		}
	}
	_, backupErr := os.Stat(backup)
	backupExists := backupErr == nil
	if backupErr != nil && !errors.Is(backupErr, os.ErrNotExist) {
		return backupErr
	}
	_, liveErr := os.Stat(live)
	if liveErr != nil && !errors.Is(liveErr, os.ErrNotExist) {
		return liveErr
	}
	// 目录已提交但记录尚未完成时只补写提交记录，保留同步期间出现的额外修改。
	if backupExists && liveErr == nil {
		if err = repo.checkAppearanceDirectory(live, pkg.Key, pkg.Files, pending.Before, true); err != nil {
			return err
		}
		return repo.applyAppearanceRecord(pending, pkg, context)
	}
	source := live
	if backupExists {
		source = backup
	}
	if err = repo.checkAppearanceDirectory(source, pkg.Key, pkg.Files, pending.Before, false); err != nil {
		return err
	}
	if err = os.RemoveAll(stage); err != nil {
		return err
	}
	if err = os.MkdirAll(stage, 0755); err != nil {
		return err
	}
	if err = repo.copyIgnoredAppearanceFiles(source, stage, pkg.Key, pending.Before, pkg.Files); err != nil {
		return err
	}
	for p, file := range pkg.Files {
		if !strings.HasPrefix(p, pkg.Key+"/") {
			continue
		}
		if err = repo.ensureFileChunks(file, context); err != nil {
			return err
		}
		copyFile := *file
		copyFile.Path = strings.TrimPrefix(p, pkg.Key)
		if err = repo.checkoutFile(&copyFile, stage, 1, 1, context); err != nil {
			return err
		}
	}
	if err = repo.checkAppearanceDirectory(stage, pkg.Key, pkg.Files, pending.Before, true); err != nil {
		return err
	}
	if err = repo.checkAppearanceDirectory(source, pkg.Key, pkg.Files, pending.Before, false); err != nil {
		return err
	}
	if err = repo.compareIgnoredAppearanceFiles(source, stage, pkg.Key, pending.Before, pkg.Files); err != nil {
		return err
	}
	if !backupExists && liveErr == nil {
		if err = os.Rename(live, backup); err != nil {
			return err
		}
		backupExists = true
	}
	if err = os.Rename(stage, live); err != nil {
		if backupExists {
			if restoreErr := os.Rename(backup, live); restoreErr != nil {
				return errors.Join(err, restoreErr)
			}
		}
		return err
	}
	return repo.applyAppearanceRecord(pending, pkg, context)
}

func (repo *Repo) applyAppearanceRecord(pending *assetApply, pkg *appearancePackage, context map[string]interface{}) error {
	p := appearanceRecordPath(pkg.Key)
	if err := repo.checkAppearanceParents(repo.absPath(p)); err != nil {
		return err
	}
	file := pkg.Files[p]
	if file != nil {
		return repo.checkoutFile(file, repo.DataPath, 1, 1, context, pending.Before[p])
	}
	if _, err := os.Stat(repo.absPath(p)); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err := repo.checkAssetBefore(p, pending.Before[p]); err != nil {
		return err
	}
	return os.Remove(repo.absPath(p))
}

func (repo *Repo) checkAppearanceParents(abs string) error {
	relative, err := filepath.Rel(repo.DataPath, abs)
	if err != nil || filepath.IsAbs(relative) || relative == ".." || strings.HasPrefix(relative, ".."+string(os.PathSeparator)) {
		return fmt.Errorf("invalid appearance package path: %s", abs)
	}
	current := strings.TrimSuffix(repo.DataPath, string(os.PathSeparator))
	for _, component := range strings.Split(relative, string(os.PathSeparator)) {
		current = filepath.Join(current, component)
		info, statErr := os.Lstat(current)
		if errors.Is(statErr, os.ErrNotExist) {
			return nil
		}
		if statErr != nil {
			return statErr
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("appearance package contains symlink: %s", current)
		}
	}
	return nil
}

// checkAppearanceDirectory 在提交前核对整包，保留外部新增、修改和符号链接，不覆盖不确定的数据。
func (repo *Repo) checkAppearanceDirectory(directory, key string, target, before map[string]*entity.File, committed bool) error {
	seen := map[string]bool{}
	matcher := repo.ignoreMatcher()
	err := filepath.WalkDir(directory, func(abs string, entry fs.DirEntry, walkErr error) error {
		if errors.Is(walkErr, os.ErrNotExist) && abs == directory {
			return nil
		}
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(directory, abs)
		if err != nil {
			return err
		}
		p := key + "/" + filepath.ToSlash(relative)
		seen[p] = true
		if entry.Type()&os.ModeSymlink != 0 {
			if before[p] == nil && target[p] == nil && (matcher.MatchesPath(p) || ignoredAppearanceRelative(relative)) {
				return nil
			}
			return fmt.Errorf("appearance package contains symlink: %s", abs)
		}
		expected := target[p]
		if !committed {
			expected = before[p]
		}
		if expected != nil {
			matches, matchErr := repo.matchesAppearanceFile(abs, expected)
			if matchErr != nil {
				return matchErr
			}
			if matches {
				return nil
			}
		}
		if !committed && target[p] != nil {
			matches, matchErr := repo.matchesAppearanceFile(abs, target[p])
			if matchErr != nil {
				return matchErr
			}
			if matches {
				return nil
			}
		}
		if expected == nil && target[p] == nil {
			info, infoErr := entry.Info()
			if infoErr != nil {
				return infoErr
			}
			ignored, ignoreErr := IgnorePath(info, abs, p, "", ".siyuan")
			if ignoreErr != nil {
				return ignoreErr
			}
			if matcher.MatchesPath(p) || ignoredAppearanceRelative(relative) || ignored {
				return nil
			}
		}
		return fmt.Errorf("%w: appearance package changed: %s", ErrIndexFileChanged, p)
	})
	if err != nil {
		return err
	}
	expected := before
	if committed {
		expected = target
	}
	for p, file := range expected {
		if file == nil || !strings.HasPrefix(p, key+"/") || seen[p] || !committed && repo.deferredVersion(file) {
			continue
		}
		// 旧版按文件恢复可能已经完成删除，目标中缺失的路径无需恢复到工作空间。
		if !committed && target[p] == nil {
			continue
		}
		return fmt.Errorf("%w: appearance package file missing: %s", ErrIndexFileChanged, p)
	}
	return nil
}

func ignoredAppearanceRelative(relative string) bool {
	parts := strings.Split(filepath.ToSlash(relative), "/")
	for i, part := range parts {
		if strings.HasPrefix(part, ".") && !(part == ".siyuan" && i < len(parts)-1) {
			return true
		}
	}
	return strings.HasSuffix(relative, ".tmp")
}

func (repo *Repo) matchesAppearanceFile(abs string, expected *entity.File) (bool, error) {
	file, err := os.Open(abs)
	if err != nil {
		return false, err
	}
	defer file.Close()
	for _, id := range expected.Chunks {
		chunk, getErr := repo.store.GetChunk(id)
		if getErr != nil {
			return false, getErr
		}
		data := make([]byte, len(chunk.Data))
		if _, err = io.ReadFull(file, data); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return false, nil
			}
			return false, err
		}
		if !bytes.Equal(data, chunk.Data) {
			return false, nil
		}
	}
	var extra [1]byte
	n, err := file.Read(extra[:])
	if err != nil && !errors.Is(err, io.EOF) {
		return false, err
	}
	return n == 0 && errors.Is(err, io.EOF), nil
}

func (repo *Repo) copyIgnoredAppearanceFiles(source, destination, key string, before, target map[string]*entity.File) error {
	return filepath.WalkDir(source, func(abs string, entry fs.DirEntry, walkErr error) error {
		if errors.Is(walkErr, os.ErrNotExist) && abs == source {
			return nil
		}
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(source, abs)
		if err != nil {
			return err
		}
		p := key + "/" + filepath.ToSlash(relative)
		if before[p] != nil || target[p] != nil {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		to := filepath.Join(destination, relative)
		if err = os.MkdirAll(filepath.Dir(to), 0755); err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			link, linkErr := os.Readlink(abs)
			if linkErr != nil {
				return linkErr
			}
			return os.Symlink(link, to)
		}
		data, err := os.ReadFile(abs)
		if err != nil {
			return err
		}
		if err = os.WriteFile(to, data, info.Mode().Perm()); err != nil {
			return err
		}
		if err = os.Chtimes(to, info.ModTime(), info.ModTime()); err != nil {
			return err
		}
		return copyAppearanceAttributes(abs, to)
	})
}

func (repo *Repo) compareIgnoredAppearanceFiles(source, stage, key string, before, target map[string]*entity.File) error {
	contents := func(directory string) (map[string][32]byte, error) {
		ret := map[string][32]byte{}
		err := filepath.WalkDir(directory, func(abs string, entry fs.DirEntry, walkErr error) error {
			if errors.Is(walkErr, os.ErrNotExist) && abs == directory {
				return nil
			}
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				return nil
			}
			relative, err := filepath.Rel(directory, abs)
			if err != nil {
				return err
			}
			p := key + "/" + filepath.ToSlash(relative)
			if before[p] != nil || target[p] != nil {
				return nil
			}
			if entry.Type()&os.ModeSymlink != 0 {
				link, linkErr := os.Readlink(abs)
				if linkErr != nil {
					return linkErr
				}
				ret[p] = sha256.Sum256([]byte("symlink:" + link))
				return nil
			}
			data, err := os.ReadFile(abs)
			if err != nil {
				return err
			}
			ret[p] = sha256.Sum256(append([]byte("file:"), data...))
			return nil
		})
		return ret, err
	}
	left, err := contents(source)
	if err != nil {
		return err
	}
	right, err := contents(stage)
	if err != nil {
		return err
	}
	if len(left) != len(right) {
		return fmt.Errorf("%w: ignored appearance files changed: %s", ErrIndexFileChanged, key)
	}
	for p, digest := range left {
		if other, exists := right[p]; !exists || other != digest {
			return fmt.Errorf("%w: ignored appearance file changed: %s", ErrIndexFileChanged, p)
		}
	}
	return nil
}

func (repo *Repo) cleanupAppearanceApply(pending *assetApply) {
	keys := map[string]bool{}
	for _, file := range append(append([]*entity.File{}, pending.Upserts...), pending.Removes...) {
		key := appearancePackageKey(file.Path)
		if key == "" || keys[key] {
			continue
		}
		keys[key] = true
		_, stage, backup, err := repo.appearanceTransactionPaths(pending.Index.ID, key)
		if err == nil {
			os.RemoveAll(stage)
			if _, statErr := os.Stat(backup); statErr == nil {
				history := filepath.Join(repo.HistoryPath,
					time.UnixMilli(pending.Index.Created).Format("2006-01-02-150405")+"-appearance-"+pending.Index.ID, key)
				if err = os.MkdirAll(filepath.Dir(history), 0755); err == nil {
					err = os.Rename(backup, history)
				}
				if err != nil {
					logging.LogWarnf("preserved appearance package backup [%s]: %s", backup, err)
				}
			}
		}
	}
}
