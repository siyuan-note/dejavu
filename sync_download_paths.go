package dejavu

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/dejavu/util"
	"github.com/siyuan-note/logging"
	"golang.org/x/text/unicode/norm"
)

const widgetPathPrefix = "/widgets/"
const downloadPathConflictPrefix = "/.siyuan-path-conflicts/"
const widgetPathConflictPrefix = "/widgets" + downloadPathConflictPrefix

type downloadPathPlan struct {
	files         []*entity.File
	targets       map[string]*entity.File
	legacyTargets map[string]string
	discarded     []*entity.File
}

func (repo *Repo) planDownloadPaths(files []*entity.File) (downloadPathPlan, error) {
	plan := downloadPathPlan{files: files}
	if !repo.repairWidgetPathsOnDownload && !repo.preferCanonicalPathsOnDownload {
		return plan, nil
	}
	aliases := map[string][]*entity.File{}
	widgetTargets := map[string]bool{}
	for _, file := range files {
		widget := repo.repairWidgetPathsOnDownload && strings.HasPrefix(file.Path, widgetPathPrefix)
		if (!widget && !repo.preferCanonicalPathsOnDownload) || !strings.Contains(file.Path, "\\") {
			continue
		}
		target := strings.ReplaceAll(file.Path, "\\", "/")
		if widget {
			rel := strings.TrimPrefix(target, widgetPathPrefix)
			if !strings.Contains(rel, "/") || !validDownloadPath(target) {
				return plan, fmt.Errorf("invalid download path %q", file.Path)
			}
			widgetTargets[target] = true
		}
		aliases[target] = append(aliases[target], file)
	}
	if len(aliases) == 0 {
		return plan, nil
	}

	plan.targets = make(map[string]*entity.File, len(aliases))
	plan.legacyTargets = make(map[string]string, len(aliases))
	canonical := map[string]*entity.File{}
	for _, file := range files {
		if _, affected := aliases[file.Path]; affected {
			if canonical[file.Path] != nil {
				return plan, fmt.Errorf("download path conflict at %q", file.Path)
			}
			canonical[file.Path] = file
		}
	}
	var repairedFiles []*entity.File
	for _, target := range slices.Sorted(maps.Keys(aliases)) {
		group := aliases[target]
		winner := canonical[target]
		// 单独的旧路径只有挂件包内路径可以按已知目录结构转换。
		if winner == nil && !widgetTargets[target] {
			continue
		}
		if !validDownloadPath(target) {
			return plan, fmt.Errorf("invalid download path %q", group[0].Path)
		}
		for _, legacy := range group {
			plan.legacyTargets[legacy.Path] = target
		}
		if winner != nil {
			plan.targets[target] = winner
			plan.discarded = append(plan.discarded, group...)
			continue
		}
		if len(group) != 1 {
			return plan, fmt.Errorf("download path conflict at %q: multiple legacy entries", target)
		}
		repaired := *group[0]
		repaired.Path = target
		// 新路径使用独立对象 ID，历史快照仍通过原 ID 读取原始记录。
		repaired.ID = util.Hash([]byte("widget-download-path\x00" + group[0].ID + "\x00" + target))
		if problem := assetFileProblem(&repaired); problem != "" {
			return plan, fmt.Errorf("invalid download file %q: %s", target, problem)
		}
		plan.targets[target] = &repaired
		repairedFiles = append(repairedFiles, &repaired)
	}
	if len(plan.targets) == 0 {
		return plan, nil
	}
	plan.files = make([]*entity.File, 0, len(files))
	for _, file := range files {
		if plan.legacyTargets[file.Path] == "" {
			plan.files = append(plan.files, file)
		}
	}
	plan.files = append(plan.files, repairedFiles...)
	if err := plan.checkTargetConflicts(); err != nil {
		return plan, err
	}
	for _, file := range repairedFiles {
		if err := repo.checkoutCanceled(); err != nil {
			return plan, err
		}
		if err := repo.store.PutFile(file); err != nil {
			return plan, err
		}
	}
	return plan, nil
}

func validDownloadPath(target string) bool {
	rel, absolute := strings.CutPrefix(target, "/")
	if !absolute || path.Clean(target) != target ||
		strings.ContainsAny(target, "\\:\x00") || !filepath.IsLocal(filepath.FromSlash(rel)) {
		return false
	}
	for part := range strings.SplitSeq(rel, "/") {
		if strings.TrimRight(part, ". ") != part {
			return false
		}
	}
	return true
}

func downloadPathKey(name string) string {
	return norm.NFC.String(strings.ToLower(name))
}

func (plan downloadPathPlan) checkTargetConflicts() error {
	files := map[string]string{}
	dirs := map[string]bool{}
	for _, file := range plan.files {
		name := downloadPathKey(file.Path)
		if previous := files[name]; previous != "" && (plan.targets[previous] != nil || plan.targets[file.Path] != nil) {
			return fmt.Errorf("download path conflict between %q and %q", previous, file.Path)
		}
		files[name] = file.Path
		for dir := path.Dir(name); dir != "/" && dir != "."; dir = path.Dir(dir) {
			dirs[dir] = true
		}
	}
	for target := range plan.targets {
		name := downloadPathKey(target)
		if dirs[name] {
			return fmt.Errorf("download path conflict at %q: file and directory", target)
		}
		for dir := path.Dir(name); dir != "/" && dir != "."; dir = path.Dir(dir) {
			if files[dir] != "" {
				return fmt.Errorf("download path conflict at %q: parent is a file", target)
			}
		}
	}
	return nil
}

func linkDownloadPathAliases(linked map[string]bool, plans ...downloadPathPlan) {
	// 本地联接遮挡任一拼写时，云端索引需要保留这个路径下的原始对象。
	for _, plan := range plans {
		for legacy, target := range plan.legacyTargets {
			if linked[legacy] {
				linked[target] = true
			}
		}
	}
	for _, plan := range plans {
		for legacy, target := range plan.legacyTargets {
			if linked[target] {
				linked[legacy] = true
			}
		}
	}
}

func (plan downloadPathPlan) checkLocalTargets(localFiles []*entity.File, links syncLinkView) error {
	if len(plan.targets) == 0 {
		return nil
	}
	targets := make(map[string]string, len(plan.targets))
	for target := range plan.targets {
		if !links.paths[target] {
			targets[downloadPathKey(target)] = target
		}
	}
	for _, file := range localFiles {
		if links.paths[file.Path] {
			continue
		}
		name := downloadPathKey(strings.ReplaceAll(file.Path, "\\", "/"))
		if target := targets[name]; target != "" && target != file.Path {
			return fmt.Errorf("download path conflict between local %q and cloud %q", file.Path, target)
		}
	}
	return nil
}

func (plan downloadPathPlan) applyDownloadDiff(merge *MergeResult, localFiles []*entity.File, links syncLinkView) ([]*entity.File, error) {
	if len(plan.targets) == 0 {
		return nil, nil
	}
	if err := plan.checkLocalTargets(localFiles, links); err != nil {
		return nil, err
	}
	current, upserts := filesByPath(localFiles), filesByPath(merge.Upserts)
	var conflicts []*entity.File
	for _, target := range slices.Sorted(maps.Keys(plan.targets)) {
		if links.paths[target] {
			continue
		}
		incoming, local := plan.targets[target], current[target]
		if upserts[target] == nil && !equalSyncFileVersion(local, incoming) {
			merge.Upserts = append(merge.Upserts, incoming)
		}
		if local != nil && !equalSyncFileVersion(local, incoming) {
			conflicts = append(conflicts, local)
		}
	}
	return conflicts, nil
}

func (repo *Repo) downloadPathIndex(plan downloadPathPlan, localFiles []*entity.File, merge *MergeResult) *entity.Index {
	if len(plan.targets) == 0 || !merge.DataChanged() {
		return nil
	}
	// 使用已落盘的对象固定本地索引，避免同大小、同时间的旧文件命中索引缓存。
	return repo.newSyncMergeIndex(appliedSyncFiles(localFiles, merge))
}

func (repo *Repo) archiveDiscardedDownloadPaths(files []*entity.File, merge *MergeResult, context map[string]interface{}) error {
	if len(files) == 0 {
		return nil
	}
	dirs, err := os.ReadDir(repo.HistoryPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	now := merge.Time.Format("2006-01-02-150405")
	for i, file := range files {
		target := strings.ReplaceAll(file.Path, "\\", "/")
		copyFile := *file
		if rel, widget := strings.CutPrefix(target, widgetPathPrefix); widget {
			copyFile.Path = widgetPathConflictPrefix + file.ID + "/" + rel
		} else {
			copyFile.Path = downloadPathConflictPrefix + file.ID + target
		}
		if problem := assetFileProblem(&copyFile); problem != "" {
			return fmt.Errorf("invalid path conflict history file %q: %s", target, problem)
		}
		found := false
		for _, dir := range dirs {
			if !dir.IsDir() || !strings.HasSuffix(dir.Name(), "-sync") {
				continue
			}
			backupPath := filepath.Join(repo.HistoryPath, dir.Name(), copyFile.Path)
			info, statErr := os.Lstat(backupPath)
			if statErr == nil && info.Mode().IsRegular() && info.Size() == copyFile.Size {
				found, err = repo.matchesFileAt(backupPath, &copyFile)
				if err != nil {
					return err
				}
				if found {
					break
				}
			}
			if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
				return statErr
			}
		}
		if found {
			continue
		}
		if err := repo.checkoutCanceled(); err != nil {
			return err
		}
		history, err := repo.getHistoryDirNow(now, "sync")
		if err != nil {
			return err
		}
		historyFile := filepath.Join(history, copyFile.Path)
		if _, err = os.Lstat(historyFile); !errors.Is(err, os.ErrNotExist) {
			if err == nil {
				return fmt.Errorf("path conflict history already exists at %q", historyFile)
			}
			return err
		}
		if repo.assetDownloads != nil {
			if err = repo.ensureFileChunks(&copyFile, context); err != nil {
				return err
			}
		}
		if err = repo.checkoutFile(&copyFile, history, i+1, len(files), context); err != nil {
			return err
		}
		logging.LogInfof("archived legacy download path [path=%q, history=%q]", file.Path, historyFile)
		merge.HistoryPaths = append(merge.HistoryPaths, copyFile.Path)
	}
	return nil
}
