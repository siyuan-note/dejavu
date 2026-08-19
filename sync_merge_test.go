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
	"fmt"
	"testing"

	"github.com/siyuan-note/dejavu/entity"
)

func TestClassifySyncFileDelta(t *testing.T) {
	base := newTestSyncFile("base", 0)
	tests := []struct {
		name    string
		base    *entity.File
		current *entity.File
		want    syncFileDelta
	}{
		{name: "missing", want: syncFileUnchanged},
		{name: "add", current: newTestSyncFile("added", 0), want: syncFileUpsert},
		{name: "remove", base: base, want: syncFileRemove},
		{name: "unchanged", base: base, current: base, want: syncFileUnchanged},
		{name: "metadata only", base: base, current: newTestSyncFile("base", 1), want: syncFileMetadataOnly},
		{name: "content in same second", base: base, current: newTestSyncFile("changed", 0), want: syncFileUpsert},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := classifySyncFileDelta(test.base, test.current); got != test.want {
				t.Fatalf("expected delta %d, got %d", test.want, got)
			}
		})
	}
}

func TestClassifySyncFileVersionsOrder(t *testing.T) {
	baseB := newTestSyncFileAtPath("/b.txt", "base", 0)
	localA := newTestSyncFileAtPath("/a.txt", "local", 1)
	localB := newTestSyncFileAtPath("/b.txt", "local", 1)
	cloudC := newTestSyncFileAtPath("/c.txt", "cloud", 1)
	versions := classifySyncFileVersions(
		[]*entity.File{baseB},
		[]*entity.File{localB, localA},
		[]*entity.File{baseB, cloudC},
	)
	if 3 != len(versions) || "/a.txt" != versions[0].Path || "/b.txt" != versions[1].Path ||
		"/c.txt" != versions[2].Path {
		t.Fatalf("unexpected classified paths: %+v", versions)
	}
	if syncFileUpsert != versions[0].LocalDelta || syncFileUnchanged != versions[0].CloudDelta ||
		syncFileUpsert != versions[1].LocalDelta || syncFileUnchanged != versions[1].CloudDelta ||
		syncFileUnchanged != versions[2].LocalDelta || syncFileUpsert != versions[2].CloudDelta {
		t.Fatalf("unexpected classified deltas: %+v", versions)
	}
}

func TestDecideSyncFile(t *testing.T) {
	base := newTestSyncFile("base", 0)
	tests := []struct {
		name         string
		base         *entity.File
		local        *entity.File
		cloud        *entity.File
		winner       syncFileWinner
		conflictType ConflictType
		history      *entity.File
		publishLocal bool
	}{
		{name: "unchanged", base: base, local: base, cloud: base},
		{name: "local update", base: base, local: newTestSyncFile("local", 1), cloud: base, winner: syncFileWinnerLocal, publishLocal: true},
		{name: "cloud update", base: base, local: base, cloud: newTestSyncFile("cloud", 1), winner: syncFileWinnerCloud},
		{name: "same update", base: base, local: newTestSyncFile("same", 1), cloud: newTestSyncFile("same", 2), winner: syncFileWinnerCloud},
		{name: "different updates", base: base, local: newTestSyncFile("local", 1), cloud: newTestSyncFile("cloud", 2), winner: syncFileWinnerLocal, conflictType: ConflictTypeLocalUpsertCloudUpsert, history: newTestSyncFile("cloud", 2), publishLocal: true},
		{name: "older local update", base: base, local: newTestSyncFile("local", 1), cloud: newTestSyncFile("cloud", 10), winner: syncFileWinnerCloud, conflictType: ConflictTypeLocalUpsertCloudUpsert, history: newTestSyncFile("local", 1)},
		{name: "local remove", base: base, cloud: base, winner: syncFileWinnerLocal, publishLocal: true},
		{name: "cloud remove", base: base, local: base, winner: syncFileWinnerCloud},
		{name: "local remove cloud update", base: base, cloud: newTestSyncFile("cloud", 1), winner: syncFileWinnerLocal, conflictType: ConflictTypeLocalRemoveCloudUpsert, history: newTestSyncFile("cloud", 1), publishLocal: true},
		{name: "local update cloud remove", base: base, local: newTestSyncFile("local", 1), winner: syncFileWinnerLocal, conflictType: ConflictTypeLocalUpsertCloudRemove, history: newTestSyncFile("local", 1), publishLocal: true},
		{name: "both remove", base: base},
		{name: "local add", local: newTestSyncFile("local", 1), winner: syncFileWinnerLocal, publishLocal: true},
		{name: "cloud add", cloud: newTestSyncFile("cloud", 1), winner: syncFileWinnerCloud},
		{name: "same add", local: newTestSyncFile("same", 1), cloud: newTestSyncFile("same", 2), winner: syncFileWinnerCloud},
		{name: "different adds", local: newTestSyncFile("local", 1), cloud: newTestSyncFile("cloud", 2), winner: syncFileWinnerLocal, conflictType: ConflictTypeLocalUpsertCloudUpsert, history: newTestSyncFile("cloud", 2), publishLocal: true},
		{name: "local metadata", base: base, local: newTestSyncFile("base", 1), cloud: base, winner: syncFileWinnerLocal, publishLocal: true},
		{name: "cloud metadata", base: base, local: base, cloud: newTestSyncFile("base", 1), winner: syncFileWinnerCloud},
		{name: "local update cloud metadata", base: base, local: newTestSyncFile("local", 1), cloud: newTestSyncFile("base", 2), winner: syncFileWinnerLocal, publishLocal: true},
		{name: "stale local update cloud metadata", base: base, local: newTestSyncFile("local", 1), cloud: newTestSyncFile("base", 10), winner: syncFileWinnerCloud, history: newTestSyncFile("local", 1)},
		{name: "local metadata cloud update", base: base, local: newTestSyncFile("base", 1), cloud: newTestSyncFile("cloud", 2), winner: syncFileWinnerCloud},
		{name: "stale local update", base: base, local: newTestSyncFile("local", -10), cloud: base, winner: syncFileWinnerCloud, history: newTestSyncFile("local", -10)},
		{name: "stale cloud update", base: base, local: newTestSyncFile("base", 10), cloud: newTestSyncFile("cloud", 1), winner: syncFileWinnerLocal, history: newTestSyncFile("cloud", 1), publishLocal: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			versions := classifySyncFileVersions(syncFileSlice(test.base), syncFileSlice(test.local), syncFileSlice(test.cloud))[0]
			decision := decideSyncFile(versions)
			if test.winner != decision.Winner || test.conflictType != decision.ConflictType ||
				test.publishLocal != decision.PublishLocal || !equalSyncFileVersion(test.history, decision.HistoryFile) {
				t.Fatalf("unexpected decision: %+v", decision)
			}
		})
	}
}

func TestSyncFileVersionTooOldBoundary(t *testing.T) {
	candidate := newTestSyncFile("candidate", 0)
	other := newTestSyncFile("other", 0)
	other.Updated = candidate.Updated + 7*60*1000
	if syncFileVersionTooOld(candidate, other) {
		t.Fatalf("exactly seven minutes must not be considered stale")
	}
	other.Updated++
	if !syncFileVersionTooOld(candidate, other) {
		t.Fatalf("more than seven minutes must be considered stale")
	}
}

func TestResolveTmpSyncFile(t *testing.T) {
	base := newTestSyncFileAtPath("/cache.tmp", "base", 0)
	local := newTestSyncFileAtPath("/cache.tmp", "local", 1)
	cloud := newTestSyncFileAtPath("/cache.tmp", "cloud", 10)
	versions := classifySyncFileVersions([]*entity.File{base}, []*entity.File{local}, []*entity.File{cloud})[0]
	decision := resolveTmpSyncFile(versions, decideSyncFile(versions))
	if syncFileWinnerLocal != decision.Winner || ConflictTypeLocalUpsertCloudUpsert != decision.ConflictType ||
		!equalSyncFileVersion(cloud, decision.HistoryFile) || !decision.PublishLocal {
		t.Fatalf("unexpected tmp decision: %+v", decision)
	}
	cloudOnly := newTestSyncFileAtPath("/cloud.tmp", "cloud", 1)
	cloudOnlyVersions := classifySyncFileVersions(nil, nil, []*entity.File{cloudOnly})[0]
	cloudOnlyDecision := resolveTmpSyncFile(cloudOnlyVersions, decideSyncFile(cloudOnlyVersions))
	if syncFileWinnerLocal != cloudOnlyDecision.Winner || "" != cloudOnlyDecision.ConflictType ||
		nil != cloudOnlyDecision.HistoryFile || cloudOnlyDecision.PublishLocal {
		t.Fatalf("cloud-only tmp file must be ignored: %+v", cloudOnlyDecision)
	}
	cloudRemoveVersions := classifySyncFileVersions([]*entity.File{base}, []*entity.File{base}, nil)[0]
	cloudRemoveDecision := decideSyncFile(cloudRemoveVersions)
	if resolved := resolveTmpSyncFile(cloudRemoveVersions, cloudRemoveDecision); syncFileWinnerCloud != resolved.Winner {
		t.Fatalf("cloud tmp remove must not be ignored: %+v", resolved)
	}
}

func TestConflictDetailCompatibility(t *testing.T) {
	base := newTestSyncFile("base", 0)
	local := newTestSyncFile("local", 1)
	cloud := newTestSyncFile("cloud", 2)
	localWinner := &ConflictDetail{Path: local.Path, Base: base, Local: local, Cloud: cloud, Winner: ConflictSideLocal}
	cloudWinner := &ConflictDetail{Path: local.Path, Base: base, Local: local, Cloud: cloud, Winner: ConflictSideCloud}
	cloudRemove := &ConflictDetail{Path: local.Path, Base: base, Local: local, Winner: ConflictSideLocal}

	if localWinner.CopyFile() != cloud || cloudWinner.CopyFile() != local || nil != cloudRemove.CopyFile() {
		t.Fatalf("unexpected conflict copy files")
	}

	result := &MergeResult{ConflictDetails: []*ConflictDetail{cloudRemove}, HistoryPaths: []string{local.Path}}
	if 1 != result.ConflictCount() || !result.DataChanged() || 0 != len(result.ConflictCopyFiles()) ||
		1 != len(result.ConflictPaths()) || local.Path != result.ConflictPaths()[0] || !result.HasHistory() {
		t.Fatalf("unexpected structured conflict result")
	}
	legacy := &MergeResult{Conflicts: []*entity.File{cloud}}
	if 1 != legacy.ConflictCount() || legacy.ConflictCopyFiles()[0] != cloud || legacy.ConflictPaths()[0] != cloud.Path ||
		!legacy.HasHistory() {
		t.Fatalf("unexpected legacy conflict result")
	}
	hybrid := &MergeResult{Conflicts: []*entity.File{cloud}, ConflictDetails: []*ConflictDetail{localWinner}}
	if 1 != hybrid.ConflictCount() || 1 != len(hybrid.ConflictCopyFiles()) || cloud != hybrid.ConflictCopyFiles()[0] ||
		1 != len(hybrid.ConflictPaths()) || local.Path != hybrid.ConflictPaths()[0] {
		t.Fatalf("unexpected hybrid conflict result")
	}
}

func newTestSyncFile(content string, minutes int) *entity.File {
	return newTestSyncFileAtPath("/doc.txt", content, minutes)
}

func newTestSyncFileAtPath(path, content string, minutes int) *entity.File {
	return &entity.File{
		ID:      fmt.Sprintf("%s-%s-%d", path, content, minutes),
		Path:    path,
		Size:    int64(len(content)),
		Updated: int64(minutes) * 60 * 1000,
		Chunks:  []string{content},
	}
}

func syncFileSlice(file *entity.File) []*entity.File {
	if nil == file {
		return nil
	}
	return []*entity.File{file}
}
