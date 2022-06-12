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

func (repo *Repo) GetLogs() (ret []*entity.Index, err error) {
	latest, err := repo.Latest()
	if nil != err {
		return
	}

	index := latest
	for i := 0; i < 64; i++ {
		ret = append(ret, index)
		hash := index.Parent
		if "" == hash {
			break
		}
		index, err = repo.store.GetIndex(hash)
		if nil != err {
			return
		}
	}
	return
}
