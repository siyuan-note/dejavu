package dejavu

import (
	"fmt"
	"path"
	"strings"

	"github.com/siyuan-note/dejavu/entity"
	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

// validateRemoteAppearanceFormat 在使用云端事件前校验协议边界，并从云端认证读取固定标记。
func (repo *Repo) validateRemoteAppearanceFormat(index *entity.Index, files []*entity.File, context map[string]interface{}) (*TrafficStat, error) {
	return repo.validateAppearanceFormat(index, files, true, context)
}

func (repo *Repo) validateAppearanceFormat(index *entity.Index, files []*entity.File, remote bool, context map[string]interface{}) (*TrafficStat, error) {
	stat := newAppearanceTraffic()
	if !repo.appearanceSyncEnabled || repo.appearanceProtocolIgnored() {
		return stat, nil
	}
	if index == nil {
		if len(files) != 0 {
			return stat, fmt.Errorf("%w: appearance files have no source index", ErrRepoFatal)
		}
		return stat, nil
	}
	const prefix = "/storage/appearance-v1"
	marker := entity.NewFile(appearanceFormatPath, int64(len(appearanceFormatData)), appearanceEventModified)
	indexIDs := map[string]bool{}
	for _, id := range index.Files {
		indexIDs[id] = true
	}
	markerPresent, eventPresent := indexIDs[marker.ID], false
	paths, keys := map[string]bool{}, map[string]string{}
	folder := cases.Fold()
	for _, file := range files {
		if file == nil {
			return stat, fmt.Errorf("%w: missing cloud file metadata", ErrRepoFatal)
		}
		lowerPath := strings.ToLower(file.Path)
		if lowerPath != prefix && !strings.HasPrefix(lowerPath, prefix+"/") {
			continue
		}
		if !strings.HasPrefix(file.Path, prefix+"/") || path.Clean(file.Path) != file.Path ||
			!validAssetFile(file) || !indexIDs[file.ID] {
			return stat, fmt.Errorf("%w: invalid appearance protocol path: %s", ErrRepoFatal, file.Path)
		}
		parts := strings.Split(strings.TrimPrefix(file.Path, prefix+"/"), "/")
		if len(parts) >= 2 {
			key := "/" + parts[0] + "/" + parts[1]
			if validAppearanceArchiveKey(key) && repo.appearanceIgnored(key) {
				continue
			}
		}
		if paths[file.Path] {
			return stat, fmt.Errorf("%w: duplicate appearance protocol path: %s", ErrRepoFatal, file.Path)
		}
		paths[file.Path] = true
		if file.Path == appearanceFormatPath {
			if file.ID != marker.ID || file.Size != marker.Size || file.Updated != marker.Updated {
				return stat, fmt.Errorf("%w: unknown appearance format marker metadata", ErrRepoFatal)
			}
			markerPresent = true
			continue
		}
		key, _ := appearanceArchiveKey(file.Path)
		if key == "" || file.ID != entity.NewFile(file.Path, file.Size, file.Updated).ID {
			return stat, fmt.Errorf("%w: unknown appearance event path or identity: %s", ErrRepoFatal, file.Path)
		}
		folded := folder.String(norm.NFC.String(key))
		if previous, exists := keys[folded]; exists && previous != key {
			return stat, fmt.Errorf("%w: appearance package path collision: %s", ErrRepoFatal, key)
		}
		keys[folded] = key
		eventPresent = true
	}
	if !markerPresent {
		if eventPresent {
			return stat, fmt.Errorf("%w: appearance events have no format marker", ErrRepoFatal)
		}
		return stat, nil
	}
	if repo.appearanceUserMatcher().MatchesPath(appearanceFormatPath) {
		return stat, fmt.Errorf("%w: appearance format marker is individually ignored", ErrRepoFatal)
	}
	// 固定旧 ID 不能证明标记内容；本地缓存即使合法，也不能替代云端源对象。
	owned, err := repo.appearanceOwnsIndex(index, remote, context, stat)
	if err != nil {
		return stat, err
	}
	if !owned {
		return stat, fmt.Errorf("%w: unsupported or invalid remote appearance format", ErrRepoFatal)
	}
	return stat, nil
}
