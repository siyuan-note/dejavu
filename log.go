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
	"time"

	"github.com/88250/gulu"
	"github.com/siyuan-note/dejavu/entity"
)

type Log struct {
	ID      string         `json:"id"`      // Hash
	Parent  string         `json:"parent"`  // 指向上一个索引
	Message string         `json:"message"` // 索引备注
	Created int64          `json:"created"` // 索引时间
	Time    string         `json:"time"`    // 格式化好的索引时间 "2006-01-02 15:04:05"
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

func (repo *Repo) GetTagLogs() (ret []*Log, err error) {
	tags := filepath.Join(repo.Path, "refs", "tags")
	if !gulu.File.IsExist(tags) {
		return
	}

	entries, err := os.ReadDir(tags)
	if nil != err {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var data []byte
		data, err = os.ReadFile(filepath.Join(tags, entry.Name()))
		if nil != err {
			return
		}
		id := string(data)
		if 40 != len(id) {
			continue
		}
		var index *entity.Index
		index, err = repo.store.GetIndex(id)
		if nil != err {
			return
		}

		var log *Log
		log, err = repo.getLog(index)
		if nil != err {
			return
		}

		ret = append(ret, log)
	}
	return
}

func (repo *Repo) GetIndexLogs(page, pageSize int) (ret []*Log, err error) {
	latest, err := repo.Latest()
	if nil != err {
		return
	}

	ret, err = repo.getLogsByParent(latest, page, pageSize)
	return
}

func (repo *Repo) getLogsByParent(index *entity.Index, page, pageSize int) (ret []*Log, err error) {
	count := 0
	var indices []*entity.Index

	indices = append(indices, index)
	for i := 1; ; i++ {
		parent := index.Parent
		if "" == parent {
			break
		}
		index, err = repo.store.GetIndex(parent)
		if nil != err {
			return
		}
		count++
		if page == count/pageSize {
			indices = append(indices, index)
		} else if page < count/pageSize {
			break
		}
	}

	for _, idx := range indices {
		var log *Log
		log, err = repo.getLog(idx)
		if nil != err {
			return
		}

		ret = append(ret, log)
	}
	return
}

func (repo *Repo) getLog(index *entity.Index) (ret *Log, err error) {
	ret = &Log{
		ID:      index.ID,
		Parent:  index.Parent,
		Message: index.Message,
		Created: index.Created,
		Time:    time.UnixMilli(index.Created).Format("2006-01-02 15:04:05"),
		Size:    index.Size,
	}

	for _, f := range index.Files {
		var file *entity.File
		file, err = repo.store.GetFile(f)
		if nil != err {
			return
		}
		ret.Files = append(ret.Files, file)
	}
	return
}