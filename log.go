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
	"github.com/dustin/go-humanize"
	"github.com/siyuan-note/dejavu/entity"
)

type Log struct {
	ID       string         `json:"id"`       // Hash
	Parent   string         `json:"parent"`   // 指向上一个索引
	Memo     string         `json:"memo"`     // 索引备注
	Created  int64          `json:"created"`  // 索引时间
	HCreated string         `json:"hCreated"` // 格式化好的索引时间 "2006-01-02 15:04:05"
	Files    []*entity.File `json:"files"`    // 文件列表
	Size     int64          `json:"size"`     // 文件总大小
	HSize    string         `json:"hSize"`    // 格式化好的文件总大小 "10.00 MB"
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

func (repo *Repo) GetIndexLogs(page, pageSize int) (ret []*Log, pageCount, totalCount int, err error) {
	index, err := repo.Latest()
	if nil != err {
		return
	}

	var indices []*entity.Index
	for i := 1; ; i++ {
		totalCount++
		currentPage := totalCount / pageSize
		if page > currentPage {
			continue
		}
		if page < currentPage {
			break
		}

		indices = append(indices, index)
		parent := index.Parent
		if "" == parent {
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
	pageCount = totalCount / pageSize
	return
}

func (repo *Repo) getLog(index *entity.Index) (ret *Log, err error) {
	ret = &Log{
		ID:       index.ID,
		Parent:   index.Parent,
		Memo:     index.Memo,
		Created:  index.Created,
		HCreated: time.UnixMilli(index.Created).Format("2006-01-02 15:04:05"),
		Size:     index.Size,
		HSize:    humanize.Bytes(uint64(index.Size)),
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
