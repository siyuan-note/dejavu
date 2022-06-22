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
	cloudFiles, err := repo.getFiles(cloudIndexes)
	if nil != err {
		return
	}

	fetchFiles, err := repo.localNotFoundFiles(cloudFiles)
	if nil != err {
		return
	}

	// TODO: 请求云端文件，获得分块
	_ = fetchFiles
	var cloudChunkIDs []string
	fetchChunks, err := repo.localNotFoundChunks(cloudChunkIDs)
	if nil != err {
		return
	}

	// TODO: 云端下载分块
	_ = fetchChunks

	// 合并云端和本地索引
	var allIndexes []*entity.Index
	allIndexes = append(allIndexes, localIndexes...)
	allIndexes = append(allIndexes, cloudIndexes...)

	// 按索引时间排序
	sort.Slice(allIndexes, func(i, j int) bool {
		return allIndexes[i].Created >= allIndexes[j].Created
	})

	// 重新排列索引
	for i := 0; i < len(allIndexes); i++ {
		index := allIndexes[i]
		if i < len(allIndexes)-1 {
			index.Parent = allIndexes[i+1].ID
		} else {
			index.Parent = latest.ID
		}
		err = repo.store.PutIndex(index)
		if nil != err {
			return
		}
	}

	latest = allIndexes[0]
	err = repo.UpdateLatest(latest.ID)
	if nil != err {
		return
	}
	return
}

func (repo *Repo) localNotFoundChunks(chunkIDs []string) (ret []string, err error) {
	for _, chunkID := range chunkIDs {
		if _, getChunkErr := repo.store.Stat(chunkID); nil != getChunkErr {
			if os.IsNotExist(getChunkErr) {
				ret = append(ret, chunkID)
				continue
			}
			err = getChunkErr
			return
		}
	}
	return
}

func (repo *Repo) localNotFoundFiles(fileIDs []string) (ret []string, err error) {
	for _, fileID := range fileIDs {
		if _, getFileErr := repo.store.Stat(fileID); nil != getFileErr {
			if os.IsNotExist(getFileErr) {
				ret = append(ret, fileID)
				continue
			}
			err = getFileErr
			return
		}
	}
	return
}

func (repo *Repo) getFiles(indexes []*entity.Index) (fileIDs []string, err error) {
	files := map[string]bool{}
	for _, index := range indexes {
		for _, file := range index.Files {
			files[file] = true
		}
	}
	for file := range files {
		fileIDs = append(fileIDs, file)
	}
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
