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
	"github.com/siyuan-note/dejavu/entity"
)

// DiffUpsertRemove 比较 left 多于/变动 right 的文件以及 left 少于 right 的文件。
func (repo *Repo) DiffUpsertRemove(left, right []*entity.File) (upserts, removes []*entity.File) {
	l := map[string]*entity.File{}
	r := map[string]*entity.File{}
	for _, f := range left {
		l[f.Path] = f
	}
	for _, f := range right {
		r[f.Path] = f
	}

	for lPath, lFile := range l {
		rFile := r[lPath]
		if nil == rFile {
			upserts = append(upserts, l[lPath])
			continue
		}
		if !equalFile(lFile, rFile) {
			upserts = append(upserts, l[lPath])
			continue
		}
	}

	for rPath := range r {
		lFile := l[rPath]
		if nil == lFile {
			removes = append(removes, r[rPath])
			continue
		}
	}
	return
}

type LeftRightDiff struct {
	LeftIndex    *entity.Index
	RightIndex   *entity.Index
	AddsLeft     []*entity.File
	UpdatesLeft  []*entity.File
	UpdatesRight []*entity.File
	RemovesRight []*entity.File
}

// DiffIndex 返回索引 left 比索引 right 新增、更新和删除的文件列表。
func (repo *Repo) DiffIndex(leftIndexID, rightIndexID string) (ret *LeftRightDiff, err error) {
	leftIndex, err := repo.GetIndex(leftIndexID)
	if nil != err {
		return
	}
	rightIndex, err := repo.GetIndex(rightIndexID)
	if nil != err {
		return
	}

	leftFiles, err := repo.getFiles(leftIndex.Files)
	if nil != err {
		return
	}
	rightFiles, err := repo.getFiles(rightIndex.Files)
	if nil != err {
		return
	}

	l := map[string]*entity.File{}
	r := map[string]*entity.File{}
	for _, f := range leftFiles {
		l[f.Path] = f
	}
	for _, f := range rightFiles {
		r[f.Path] = f
	}

	ret = &LeftRightDiff{
		LeftIndex:  leftIndex,
		RightIndex: rightIndex,
	}

	for lPath, lFile := range l {
		rFile := r[lPath]
		if nil == rFile {
			ret.AddsLeft = append(ret.AddsLeft, l[lPath])
			continue
		}
		if !equalFile(lFile, rFile) {
			ret.UpdatesLeft = append(ret.UpdatesLeft, l[lPath])
			ret.UpdatesRight = append(ret.UpdatesRight, r[lPath])
			continue
		}
	}

	for rPath := range r {
		lFile := l[rPath]
		if nil == lFile {
			ret.RemovesRight = append(ret.RemovesRight, r[rPath])
			continue
		}
	}
	return
}

func equalFile(left, right *entity.File) bool {
	if left.Path != right.Path {
		return false
	}
	if left.Updated/1000 != right.Updated/1000 { // Improve data sync file timestamp comparison https://github.com/siyuan-note/siyuan/issues/8573
		return false
	}
	return true
}
