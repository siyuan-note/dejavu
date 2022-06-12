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
	"github.com/88250/gulu"
	"github.com/siyuan-note/dejavu/entity"
)

type Log struct {
	ID      string         `json:"id"`      // Hash
	Parent  string         `json:"parent"`  // 指向上一个索引
	Message string         `json:"message"` // 索引备注
	Created int64          `json:"created"` // 索引时间
	Files   []*entity.File `json:"files"`   // 文件列表
	Size    int64          `json:"size"`    // 文件总大小
}

func (log *Log) String() string {
	data, err := gulu.JSON.MarshalJSON(log)
	if nil != err {
		return "print log [" + log.ID + "] failed"
	}
	return string(data)
}

func (repo *Repo) GetLogs() (ret []*Log, err error) {
	latest, err := repo.Latest()
	if nil != err {
		return
	}

	index := latest
	for i := 0; i < 64; i++ {
		log := &Log{
			ID:      index.ID,
			Parent:  index.Parent,
			Message: index.Message,
			Created: index.Created,
			Size:    index.Size,
		}

		for _, f := range index.Files {
			var file *entity.File
			file, err = repo.store.GetFile(f)
			if nil != err {
				return
			}
			log.Files = append(log.Files, file)
		}

		ret = append(ret, log)
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
