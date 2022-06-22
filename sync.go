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

import (
	"os"
	"path/filepath"
	"sort"

	"github.com/88250/gulu"
	"github.com/siyuan-note/dejavu/entity"
)

func (repo *Repo) Sync() (err error) {
	// TODO: 先打快照

	latest, err := repo.Latest()
	if nil != err {
		return
	}

	latestSync, err := repo.latestSync(latest)
	if nil != err {
		return
	}

	localIndexes, err := repo.getIndexes(latest.ID, latestSync.ID)
	if nil != err {
		return
	}

	// TODO: 请求云端返回索引列表
	_ = latestSync

	var cloudIndexes []*entity.Index
	var allIndexes []*entity.Index
	// 合并所有索引，然后按索引时间排序
	allIndexes = append(allIndexes, localIndexes...)
	allIndexes = append(allIndexes, cloudIndexes...)
	sort.Slice(allIndexes, func(i, j int) bool {
		return allIndexes[i].Created >= allIndexes[j].Created
	})

	return
}

// latestSync 获取最近一次同步点。
func (repo *Repo) latestSync(latest *entity.Index) (ret *entity.Index, err error) {
	latestSync := filepath.Join(repo.Path, "refs", "latest-sync")
	if !gulu.File.IsExist(latestSync) {
		// 使用第一个索引作为同步点
		ret, err = repo.getInitIndex(latest)
		return
	}

	data, err := os.ReadFile(latestSync)
	if nil != err {
		return
	}
	hash := string(data)
	ret, err = repo.store.GetIndex(hash)
	return
}
