// DejaVu - Data snapshot and sync.
// Copyright (c) 2022-present, b3log.org
//
// DejaVu is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//         http://license.coscl.org.cn/MulanPSL2
//
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
//
// See the Mulan PSL v2 for more details.

package dejavu

import "github.com/siyuan-note/dejavu/entity"

// DiffUpsert 比较 left 多于/变动 right 的文件。
func (repo *Repo) DiffUpsert(left, right []*entity.File) (ret []*entity.File) {
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
			ret = append(ret, l[lPath])
			continue
		}
		if lFile.Updated != rFile.Updated || lFile.Path != rFile.Path {
			ret = append(ret, l[lPath])
			continue
		}
	}
	return
}

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
		if lFile.Updated != rFile.Updated || lFile.Path != rFile.Path {
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
