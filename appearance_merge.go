package dejavu

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/siyuan-note/dejavu/entity"
)

func (repo *Repo) appearanceProtocolFileIgnored(p string) bool {
	if !repo.appearanceSyncEnabled {
		return false
	}
	const root = "/storage/appearance-v1"
	lower := strings.ToLower(p)
	if lower != root && !strings.HasPrefix(lower, root+"/") {
		return false
	}
	if repo.appearanceProtocolIgnored() {
		return true
	}
	key := appearanceEventContainerKey(p)
	return key != "" && repo.appearanceIgnored(key)
}

func hasAppearanceProjection(pending *assetApply) bool {
	for _, file := range append(append([]*entity.File{}, pending.Upserts...), pending.Removes...) {
		if appearancePackageKey(file.Path) != "" {
			return true
		}
	}
	return false
}

// matchesManagedAppearanceIgnore 识别隔离回调已完成的结果，重放时保留新内容及其新时间戳。
func (repo *Repo) matchesManagedAppearanceIgnore(expected *entity.File) (bool, error) {
	current, err := os.ReadFile(repo.absPath(repo.ignoreRulePath))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !strings.Contains(string(current), "# siyuan-appearance-isolation:v1:begin") {
		return false, nil
	}
	currentLines, err := appearanceUserIgnoreLines(strings.Split(string(current), "\n"))
	if err != nil {
		return false, err
	}
	var wanted []byte
	if expected != nil {
		wanted, err = repo.openFile(expected)
		if err != nil {
			return false, err
		}
	}
	expectedLines, err := appearanceUserIgnoreLines(strings.Split(string(wanted), "\n"))
	if err != nil {
		return false, err
	}
	return strings.TrimRight(strings.Join(currentLines, "\n"), "\n") ==
		strings.TrimRight(strings.Join(expectedLines, "\n"), "\n"), nil
}

func appearanceUserIgnoreLines(lines []string) ([]string, error) {
	block := []string{"# siyuan-appearance-isolation:v1:begin", "/themes/", "/icons/",
		"/storage/bazaar/themes/", "/storage/bazaar/icons/", "# siyuan-appearance-isolation:v1:end"}
	var ret []string
	for i := 0; i < len(lines); i++ {
		line := strings.TrimSuffix(lines[i], "\r")
		if i == 0 {
			line = strings.TrimPrefix(line, "\ufeff")
		}
		if !strings.HasPrefix(line, "# siyuan-appearance-isolation:") {
			ret = append(ret, line)
			continue
		}
		if len(lines)-i < len(block) {
			return nil, errors.New("incomplete appearance sync isolation block")
		}
		for j, expected := range block {
			actual := strings.TrimSuffix(lines[i+j], "\r")
			if i+j == 0 {
				actual = strings.TrimPrefix(actual, "\ufeff")
			}
			if actual != expected {
				return nil, errors.New("invalid or unsupported appearance sync isolation block")
			}
		}
		i += len(block) - 1
	}
	return ret, nil
}

func (repo *Repo) selectAppearanceIgnoreRules(base, local, remote []*entity.File, context map[string]interface{}, mode string) error {
	for _, versions := range classifySyncFileVersions(base, local, remote) {
		if versions.Path != repo.ignoreRulePath {
			continue
		}
		selected := versions.Local
		if mode == "download" || mode != "upload" && decideSyncFile(versions).Winner == syncFileWinnerCloud {
			selected = versions.Cloud
		}
		if selected == nil {
			repo.appearanceIgnoreLines = nil
			return nil
		}
		if err := repo.ensureFileChunks(selected, context); err != nil {
			return err
		}
		data, err := repo.openFile(selected)
		if err != nil {
			return err
		}
		repo.appearanceIgnoreLines, err = appearanceUserIgnoreLines(strings.Split(string(data), "\n"))
		return err
	}
	return nil
}

func (repo *Repo) prepareAppearanceSyncIgnore(latest *entity.Index, cloudFiles []*entity.File, context map[string]interface{}, mode string) error {
	if !repo.appearanceSyncEnabled {
		return nil
	}
	local, err := repo.getFiles(latest.Files)
	if err != nil {
		return err
	}
	base, err := repo.getFiles(repo.latestSync().Files)
	if err != nil {
		return err
	}
	return repo.selectAppearanceIgnoreRules(base, local, cloudFiles, context, mode)
}

func (repo *Repo) mergeAppearanceEventFiles(baseFiles, localFiles, cloudFiles []*entity.File,
	result *MergeResult, traffic *TrafficStat, context map[string]interface{}, mode string) ([]*entity.File, bool, error) {
	if !repo.appearanceSyncEnabled {
		return cloudFiles, false, nil
	}
	if err := repo.selectAppearanceIgnoreRules(baseFiles, localFiles, cloudFiles, context, mode); err != nil {
		return nil, false, err
	}
	var rootFiles []*entity.File
	if !repo.appearanceProtocolIgnored() {
		root, rootTraffic, err := repo.readAppearanceRecoveryRoot(context)
		addAppearanceTraffic(traffic, rootTraffic)
		if err != nil {
			return nil, false, err
		}
		if root != nil {
			rootFiles, err = repo.getFiles(root.Files)
			if err != nil {
				return nil, false, err
			}
		}
	}
	base, err := repo.readAppearanceEvents(baseFiles, context)
	if err != nil {
		return nil, false, err
	}
	local, err := repo.readAppearanceEvents(localFiles, context)
	if err != nil {
		return nil, false, err
	}
	cloud, err := repo.readAppearanceEvents(append(append([]*entity.File{}, cloudFiles...), rootFiles...), context)
	if err != nil {
		return nil, false, err
	}
	events, err := unionAppearanceEvents(base, local, cloud)
	if err != nil {
		return nil, false, err
	}
	ret := map[string]*entity.File{}
	actualCloud := filesByPath(cloudFiles)
	for _, file := range cloudFiles {
		if repo.appearanceProtocolFileIgnored(file.Path) {
			continue
		}
		if key, _ := appearanceArchiveKey(file.Path); key != "" {
			continue
		}
		if file.Path == appearanceFormatPath && repo.appearanceProtocolIgnored() {
			continue
		}
		ret[file.Path] = file
	}
	publish := false
	for key, packageEvents := range events {
		if repo.appearanceIgnored(key) {
			continue
		}
		for _, event := range packageEvents {
			if repo.appearanceUserMatcher().MatchesPath(event.File.Path) {
				return nil, false, fmt.Errorf("appearance event is individually ignored: %s", event.File.Path)
			}
			ret[event.File.Path] = event.File
			if actualCloud[event.File.Path] == nil {
				publish = true
			}
		}
		heads := appearanceEventHeads(packageEvents)
		selected := selectAppearanceEvent(packageEvents, cloud[key])
		if mode == "download" {
			if remoteHead := selectAppearanceEvent(cloud[key], nil); remoteHead != nil {
				selected = remoteHead
			}
		} else if mode == "upload" {
			if localHead := selectAppearanceEvent(local[key], nil); localHead != nil {
				selected = localHead
			}
		}
		if len(heads) < 2 && (len(heads) == 0 || selected.Digest == heads[0].Digest) {
			continue
		}
		resolved := &appearanceArchive{Key: key, State: selected.State, Files: selected.Files}
		for _, head := range heads {
			resolved.Parents = append(resolved.Parents, head.Digest)
		}
		data, encodeErr := encodeAppearanceArchive(resolved)
		if encodeErr != nil {
			return nil, false, encodeErr
		}
		file, putErr := repo.putAppearanceBytes(appearanceArchivePath(resolved), data, true)
		if putErr != nil {
			return nil, false, putErr
		}
		ret[file.Path] = file
		publish = true
		localHead := selectAppearanceEvent(local[key], nil)
		if localHead != nil && !sameAppearanceProjection(localHead.appearanceArchive, selected.appearanceArchive) {
			if err = repo.preserveAppearanceConflict(localHead, selected, result, context); err != nil {
				return nil, false, err
			}
		}
	}
	if !repo.appearanceProtocolIgnored() {
		for _, file := range append(append(append([]*entity.File{}, localFiles...), rootFiles...), cloudFiles...) {
			if file.Path == appearanceFormatPath {
				ret[file.Path] = file
				if actualCloud[file.Path] == nil {
					publish = true
				}
				break
			}
		}
	}
	var files []*entity.File
	for _, file := range ret {
		files = append(files, file)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, publish, nil
}

func (repo *Repo) preserveAppearanceConflict(local, winner *appearanceEvent, result *MergeResult, context map[string]interface{}) error {
	files, err := repo.appearanceArchiveProjectionFiles(local.appearanceArchive)
	if err != nil {
		return err
	}
	now := result.Time.Format("2006-01-02-150405")
	temp := filepath.Join(repo.TempPath, "repo", "sync", "conflicts", now)
	for _, file := range files {
		if err = repo.checkoutFile(file, temp, 1, len(files), context); err != nil {
			return err
		}
		if err = repo.genSyncHistory(now, file.Path, filepath.Join(temp, file.Path)); err != nil {
			return err
		}
		result.HistoryPaths = append(result.HistoryPaths, file.Path)
	}
	kind := ConflictTypeLocalUpsertCloudUpsert
	if appearanceEventRecord(winner).Deleted {
		kind = ConflictTypeLocalUpsertCloudRemove
	}
	result.ConflictDetails = append(result.ConflictDetails, &ConflictDetail{Path: local.Key, Type: kind,
		Winner: conflictSide(syncFileWinnerCloud)})
	return nil
}
