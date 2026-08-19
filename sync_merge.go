// DejaVu - Data snapshot and sync.
// Copyright (c) 2022-present, b3log.org
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package dejavu

import (
	"sort"
	"strings"

	"github.com/siyuan-note/dejavu/entity"
)

type ConflictType string

const (
	ConflictTypeLocalUpsertCloudUpsert ConflictType = "local-upsert-cloud-upsert"
	ConflictTypeLocalRemoveCloudUpsert ConflictType = "local-remove-cloud-upsert"
	ConflictTypeLocalUpsertCloudRemove ConflictType = "local-upsert-cloud-remove"
)

type ConflictSide string

const (
	ConflictSideLocal ConflictSide = "local"
	ConflictSideCloud ConflictSide = "cloud"
)

// ConflictDetail 描述同一路径在上次同步、本地和云端的冲突版本及最终采用的一侧。
type ConflictDetail struct {
	Path   string
	Type   ConflictType
	Base   *entity.File
	Local  *entity.File
	Cloud  *entity.File
	Winner ConflictSide
}

// CopyFile 返回需要生成冲突副本的未采用版本，返回 nil 表示只记录和提示冲突。
func (detail *ConflictDetail) CopyFile() *entity.File {
	if ConflictSideCloud == detail.Winner {
		return detail.Local
	}
	return detail.Cloud
}

type syncFileDelta uint8

const (
	syncFileUnchanged syncFileDelta = iota
	syncFileMetadataOnly
	syncFileUpsert
	syncFileRemove
)

func (delta syncFileDelta) contentChanged() bool {
	return syncFileUpsert == delta || syncFileRemove == delta
}

type syncFileVersions struct {
	Path               string
	Base, Local, Cloud *entity.File
	LocalDelta         syncFileDelta
	CloudDelta         syncFileDelta
}

type syncFileWinner uint8

const (
	syncFileWinnerNone syncFileWinner = iota
	syncFileWinnerLocal
	syncFileWinnerCloud
)

type syncFileDecision struct {
	Winner       syncFileWinner
	ConflictType ConflictType
	HistoryFile  *entity.File
	PublishLocal bool
}

func classifySyncFileDelta(base, current *entity.File) syncFileDelta {
	if nil == base {
		if nil == current {
			return syncFileUnchanged
		}
		return syncFileUpsert
	}
	if nil == current {
		return syncFileRemove
	}
	if !equalFileContent(base, current) {
		return syncFileUpsert
	}
	if !equalFile(base, current) {
		return syncFileMetadataOnly
	}
	return syncFileUnchanged
}

func classifySyncFileVersions(baseFiles, localFiles, cloudFiles []*entity.File) []*syncFileVersions {
	baseByPath := filesByPath(baseFiles)
	localByPath := filesByPath(localFiles)
	cloudByPath := filesByPath(cloudFiles)
	pathSet := map[string]bool{}
	for path := range baseByPath {
		pathSet[path] = true
	}
	for path := range localByPath {
		pathSet[path] = true
	}
	for path := range cloudByPath {
		pathSet[path] = true
	}

	paths := make([]string, 0, len(pathSet))
	for path := range pathSet {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	ret := make([]*syncFileVersions, 0, len(paths))
	for _, path := range paths {
		base, local, cloud := baseByPath[path], localByPath[path], cloudByPath[path]
		ret = append(ret, &syncFileVersions{
			Path:       path,
			Base:       base,
			Local:      local,
			Cloud:      cloud,
			LocalDelta: classifySyncFileDelta(base, local),
			CloudDelta: classifySyncFileDelta(base, cloud),
		})
	}
	return ret
}

func decideSyncFile(versions *syncFileVersions) syncFileDecision {
	if equalFileContent(versions.Local, versions.Cloud) {
		return decideSameContentSyncFile(versions)
	}

	localContentChanged := versions.LocalDelta.contentChanged()
	cloudContentChanged := versions.CloudDelta.contentChanged()
	if !localContentChanged && !cloudContentChanged {
		return decideSameContentSyncFile(versions)
	}

	if localContentChanged && cloudContentChanged {
		return decideConflictedSyncFile(versions)
	}

	if localContentChanged {
		if syncFileVersionTooOld(versions.Local, versions.Cloud) {
			return syncFileDecision{Winner: syncFileWinnerCloud, HistoryFile: versions.Local}
		}
		return syncFileDecision{Winner: syncFileWinnerLocal, PublishLocal: true}
	}

	if syncFileVersionTooOld(versions.Cloud, versions.Local) {
		return syncFileDecision{Winner: syncFileWinnerLocal, HistoryFile: versions.Cloud, PublishLocal: true}
	}
	return syncFileDecision{Winner: syncFileWinnerCloud}
}

func decideSameContentSyncFile(versions *syncFileVersions) syncFileDecision {
	if syncFileUnchanged == versions.LocalDelta && syncFileUnchanged == versions.CloudDelta {
		return syncFileDecision{}
	}
	if nil == versions.Local && nil == versions.Cloud {
		return syncFileDecision{}
	}
	if preferCloudMetadata(versions.Local, versions.Cloud) {
		return syncFileDecision{Winner: syncFileWinnerCloud}
	}
	return syncFileDecision{Winner: syncFileWinnerLocal, PublishLocal: true}
}

func decideConflictedSyncFile(versions *syncFileVersions) syncFileDecision {
	if nil == versions.Local {
		return syncFileDecision{
			Winner:       syncFileWinnerLocal,
			ConflictType: ConflictTypeLocalRemoveCloudUpsert,
			HistoryFile:  versions.Cloud,
			PublishLocal: true,
		}
	}
	if nil == versions.Cloud {
		return syncFileDecision{
			Winner:       syncFileWinnerLocal,
			ConflictType: ConflictTypeLocalUpsertCloudRemove,
			HistoryFile:  versions.Local,
			PublishLocal: true,
		}
	}
	if syncFileVersionTooOld(versions.Local, versions.Cloud) {
		return syncFileDecision{
			Winner:       syncFileWinnerCloud,
			ConflictType: ConflictTypeLocalUpsertCloudUpsert,
			HistoryFile:  versions.Local,
		}
	}
	return syncFileDecision{
		Winner:       syncFileWinnerLocal,
		ConflictType: ConflictTypeLocalUpsertCloudUpsert,
		HistoryFile:  versions.Cloud,
		PublishLocal: true,
	}
}

// resolveTmpSyncFile 避免将云端 `.tmp` 临时文件迁出到数据目录 https://github.com/siyuan-note/siyuan/issues/7087
func resolveTmpSyncFile(versions *syncFileVersions, decision syncFileDecision) syncFileDecision {
	if syncFileWinnerCloud != decision.Winner || nil == versions.Cloud || !strings.HasSuffix(versions.Path, ".tmp") {
		return decision
	}
	ret := syncFileDecision{Winner: syncFileWinnerLocal}
	if "" != decision.ConflictType {
		ret.ConflictType = decision.ConflictType
		ret.HistoryFile = versions.Cloud
		ret.PublishLocal = true
	}
	return ret
}

func syncFileVersionTooOld(candidate, other *entity.File) bool {
	return nil != candidate && nil != other && candidate.Updated < other.Updated-7*60*1000
}

func preferCloudMetadata(local, cloud *entity.File) bool {
	if nil == cloud {
		return false
	}
	if nil == local {
		return true
	}
	if cloud.Updated != local.Updated {
		return cloud.Updated > local.Updated
	}
	return cloud.ID > local.ID
}

func equalSyncFileVersion(left, right *entity.File) bool {
	if nil == left || nil == right {
		return left == right
	}
	return equalFile(left, right) && equalFileContent(left, right)
}

func conflictSide(winner syncFileWinner) ConflictSide {
	if syncFileWinnerCloud == winner {
		return ConflictSideCloud
	}
	return ConflictSideLocal
}

func appendUniqueSyncFile(files []*entity.File, file *entity.File) []*entity.File {
	for _, current := range files {
		if current.ID == file.ID && current.Path == file.Path {
			return files
		}
	}
	return append(files, file)
}

func filesByPath(files []*entity.File) map[string]*entity.File {
	ret := make(map[string]*entity.File, len(files))
	for _, file := range files {
		ret[file.Path] = file
	}
	return ret
}
