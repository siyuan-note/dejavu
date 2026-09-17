package dejavu

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"

	ignore "github.com/sabhiram/go-gitignore"
	"github.com/siyuan-note/dejavu/entity"
)

// appearanceRecord 将安装状态与完整包文件绑定，旧快照没有此记录时仍按完整目录参与合并。
type appearanceRecord struct {
	Version   int               `json:"version"`
	Deleted   bool              `json:"deleted"`
	Migration bool              `json:"migration"`
	Files     map[string]string `json:"files"`
}

type appearancePackage struct {
	Key    string
	Files  map[string]*entity.File
	Record *appearanceRecord
}

func appearancePackageKey(p string) string {
	if path.Clean(p) != p || strings.ContainsAny(p, "\\:\x00") {
		return ""
	}
	parts := strings.Split(strings.TrimPrefix(p, "/"), "/")
	if len(parts) >= 3 && (parts[0] == "themes" || parts[0] == "icons") && validAppearanceName(parts[1]) {
		return "/" + parts[0] + "/" + parts[1]
	}
	if len(parts) == 4 && parts[0] == "storage" && parts[1] == "bazaar" &&
		(parts[2] == "themes" || parts[2] == "icons") && strings.HasSuffix(parts[3], ".json") {
		name := strings.TrimSuffix(parts[3], ".json")
		if validAppearanceName(name) {
			return "/" + parts[2] + "/" + name
		}
	}
	return ""
}

func validAppearanceName(name string) bool {
	return name != "" && !strings.HasPrefix(name, ".") && !strings.ContainsAny(name, "/\\:\x00")
}

func appearanceRecordPath(key string) string {
	return "/storage/bazaar" + key + ".json"
}

func appearancePackages(files []*entity.File) map[string]*appearancePackage {
	ret := map[string]*appearancePackage{}
	for _, file := range files {
		key := appearancePackageKey(file.Path)
		if key == "" {
			continue
		}
		pkg := ret[key]
		if pkg == nil {
			pkg = &appearancePackage{Key: key, Files: map[string]*entity.File{}}
			ret[key] = pkg
		}
		pkg.Files[file.Path] = file
	}
	return ret
}

func (repo *Repo) validateAppearancePackages(files []*entity.File, context map[string]interface{}) (map[string]*appearancePackage, error) {
	packages := appearancePackages(files)
	caseKeys := map[string]string{}
	for key, pkg := range packages {
		folded := strings.ToLower(key)
		if previous := caseKeys[folded]; previous != "" && previous != key {
			return nil, fmt.Errorf("appearance package name collision: %s and %s", previous, key)
		}
		caseKeys[folded] = key
		if err := repo.validateAppearancePackage(pkg, context); err != nil {
			return nil, err
		}
	}
	return packages, nil
}

func (repo *Repo) validateIndexedAppearance(files []*entity.File, context map[string]interface{}) error {
	if !repo.appearanceSyncEnabled {
		return nil
	}
	var eventFiles []*entity.File
	for _, file := range files {
		if key, _ := appearanceArchiveKey(file.Path); key != "" {
			full, err := repo.store.GetFile(file.ID)
			if err != nil {
				return err
			}
			eventFiles = append(eventFiles, full)
		}
	}
	if len(eventFiles) != 0 {
		events, err := repo.readAppearanceEvents(eventFiles, context)
		if err != nil {
			return err
		}
		_, err = unionAppearanceEvents(events)
		return err
	}
	var stored []*entity.File
	for _, file := range files {
		if appearancePackageKey(file.Path) == "" {
			continue
		}
		full, err := repo.store.GetFile(file.ID)
		if err != nil {
			return err
		}
		stored = append(stored, full)
	}
	packages, err := repo.validateAppearancePackages(stored, context)
	if err != nil {
		return err
	}
	for key, pkg := range packages {
		if pkg.Record != nil {
			continue
		}
		p := appearanceRecordPath(key)
		if _, statErr := os.Lstat(repo.absPath(p)); statErr == nil {
			return fmt.Errorf("appearance package record is excluded from snapshot: %s", p)
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return statErr
		}
	}
	return nil
}

func (repo *Repo) validateAppearancePackage(pkg *appearancePackage, context map[string]interface{}) error {
	recordFile := pkg.Files[appearanceRecordPath(pkg.Key)]
	if recordFile == nil {
		return nil
	}
	if err := repo.ensureFileChunks(recordFile, context); err != nil {
		return err
	}
	data, err := repo.openFile(recordFile)
	if err != nil {
		return err
	}
	var record appearanceRecord
	if err = json.Unmarshal(data, &record); err != nil || record.Version != 1 || record.Files == nil {
		return fmt.Errorf("invalid appearance package record: %s", recordFile.Path)
	}
	if record.Deleted && (len(record.Files) != 0 || len(pkg.Files) != 1) {
		return fmt.Errorf("deleted appearance package contains files: %s", pkg.Key)
	}
	if !record.Deleted && (len(record.Files) == 0 || len(pkg.Files) != len(record.Files)+1) {
		return fmt.Errorf("incomplete appearance package: %s", pkg.Key)
	}
	foldedPaths := map[string]bool{}
	for relative, digest := range record.Files {
		if relative == "" || relative == "." || strings.HasPrefix(relative, "/") ||
			path.Clean(relative) != relative || strings.HasPrefix(relative, "../") ||
			strings.ContainsAny(relative, "\\:\x00") || ignoredAppearanceRelative(relative, false) ||
			len(digest) != 64 || strings.ToLower(digest) != digest {
			return fmt.Errorf("invalid appearance package file: %s/%s", pkg.Key, relative)
		}
		if _, err = hex.DecodeString(digest); err != nil {
			return fmt.Errorf("invalid appearance package digest: %s/%s", pkg.Key, relative)
		}
		folded := strings.ToLower(relative)
		if foldedPaths[folded] {
			return fmt.Errorf("appearance package path collision: %s/%s", pkg.Key, relative)
		}
		foldedPaths[folded] = true
		file := pkg.Files[pkg.Key+"/"+relative]
		if file == nil {
			return fmt.Errorf("incomplete appearance package: %s/%s", pkg.Key, relative)
		}
		if err = repo.ensureFileChunks(file, context); err != nil {
			return err
		}
		hash := sha256.New()
		for _, id := range file.Chunks {
			chunk, getErr := repo.store.GetChunk(id)
			if getErr != nil {
				return getErr
			}
			hash.Write(chunk.Data)
		}
		if hex.EncodeToString(hash.Sum(nil)) != digest {
			return fmt.Errorf("appearance package digest mismatch: %s", file.Path)
		}
	}
	pkg.Record = &record
	return nil
}

func equalAppearanceContent(left, right *appearancePackage) bool {
	if left == nil || right == nil {
		return left == right
	}
	if len(left.Files) != len(right.Files) {
		return false
	}
	for p, file := range left.Files {
		if !equalFileContent(file, right.Files[p]) {
			return false
		}
	}
	return true
}

func appearanceDeleted(pkg *appearancePackage) bool {
	return pkg == nil || pkg.Record != nil && pkg.Record.Deleted
}

func appearanceWholeIgnored(matcher *ignore.GitIgnore, key string) bool {
	return matcher.MatchesPath(key) || matcher.MatchesPath(key+"/")
}

func (repo *Repo) appearanceSyncIgnored(matcher *ignore.GitIgnore, p string) bool {
	if matcher.MatchesPath(p) {
		return true
	}
	if !repo.appearanceSyncEnabled {
		return false
	}
	key := appearancePackageKey(p)
	if key == "" {
		key, _ = appearanceArchiveKey(p)
	}
	return key != "" && appearanceWholeIgnored(matcher, key)
}

func decideAppearancePackage(base, local, cloud *appearancePackage) syncFileDecision {
	if local != nil && local.Record != nil && local.Record.Deleted && cloud == nil {
		return syncFileDecision{Winner: syncFileWinnerLocal, PublishLocal: true}
	}
	if cloud != nil && cloud.Record != nil && cloud.Record.Deleted && local == nil {
		return syncFileDecision{Winner: syncFileWinnerCloud}
	}
	if equalAppearanceContent(local, cloud) {
		// 内容相同时统一采用云端元数据，避免包内各文件分别选择时间戳。
		return syncFileDecision{Winner: syncFileWinnerCloud}
	}
	localChanged, cloudChanged := !equalAppearanceContent(base, local), !equalAppearanceContent(base, cloud)
	if !localChanged {
		return syncFileDecision{Winner: syncFileWinnerCloud}
	}
	if !cloudChanged {
		return syncFileDecision{Winner: syncFileWinnerLocal, PublishLocal: true}
	}
	if appearanceDeleted(local) && appearanceDeleted(cloud) {
		return syncFileDecision{Winner: syncFileWinnerCloud}
	}
	if appearanceDeleted(local) {
		return syncFileDecision{Winner: syncFileWinnerLocal, PublishLocal: true, ConflictType: ConflictTypeLocalRemoveCloudUpsert}
	}
	if appearanceDeleted(cloud) {
		return syncFileDecision{Winner: syncFileWinnerCloud, ConflictType: ConflictTypeLocalUpsertCloudRemove}
	}
	return syncFileDecision{Winner: syncFileWinnerCloud, ConflictType: ConflictTypeLocalUpsertCloudUpsert}
}

func (repo *Repo) appearanceDecisions(baseFiles, localFiles, cloudFiles []*entity.File, context map[string]interface{}) (map[string]syncFileDecision, error) {
	if !repo.appearanceSyncEnabled {
		return map[string]syncFileDecision{}, nil
	}
	base, err := repo.validateAppearancePackages(baseFiles, context)
	if err != nil {
		return nil, err
	}
	local, err := repo.validateAppearancePackages(localFiles, context)
	if err != nil {
		return nil, err
	}
	cloud, err := repo.validateAppearancePackages(cloudFiles, context)
	if err != nil {
		return nil, err
	}
	cloudIgnore, err := repo.cloudAssetIgnoreMatcher(cloudFiles, context)
	if err != nil {
		return nil, err
	}
	localIgnore := repo.ignoreMatcher()
	keys := map[string]bool{}
	for _, packages := range []map[string]*appearancePackage{base, local, cloud} {
		for key := range packages {
			keys[key] = true
		}
	}
	ret := map[string]syncFileDecision{}
	for key := range keys {
		localIgnored := local[key] == nil && appearanceWholeIgnored(localIgnore, key)
		cloudIgnored := cloud[key] == nil && appearanceWholeIgnored(cloudIgnore, key)
		if base[key] != nil && base[key].Record != nil {
			for i, current := range []*appearancePackage{local[key], cloud[key]} {
				ignored := i == 0 && localIgnored || i == 1 && cloudIgnored
				other := cloud[key]
				if i == 1 {
					other = local[key]
				}
				retainsTombstone := current == nil && other != nil && other.Record != nil && other.Record.Deleted
				if !ignored && !retainsTombstone && (current == nil || current.Record == nil) {
					return nil, fmt.Errorf("missing appearance package record: %s", key)
				}
			}
		}
		decision := decideAppearancePackage(base[key], local[key], cloud[key])
		if localIgnored {
			decision = syncFileDecision{Winner: syncFileWinnerLocal, PublishLocal: true}
		} else if cloudIgnored {
			decision = syncFileDecision{Winner: syncFileWinnerCloud}
		}
		for _, packages := range []map[string]*appearancePackage{base, local, cloud} {
			pkg := packages[key]
			if pkg == nil {
				continue
			}
			for p := range pkg.Files {
				fileDecision := decision
				if decision.ConflictType != "" {
					loser := local[key]
					if decision.Winner == syncFileWinnerLocal {
						loser = cloud[key]
					}
					if loser != nil {
						fileDecision.HistoryFile = loser.Files[p]
					}
				}
				ret[p] = fileDecision
			}
		}
	}
	return ret, nil
}

func appearanceFiles(packages map[string]*appearancePackage) []*entity.File {
	var ret []*entity.File
	for _, pkg := range packages {
		for _, file := range pkg.Files {
			ret = append(ret, file)
		}
	}
	sort.Slice(ret, func(i, j int) bool { return ret[i].Path < ret[j].Path })
	return ret
}
