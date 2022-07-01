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
	"math"
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
	Count    int            `json:"count"`    // 文件总数
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

	var indexes []*entity.Index
	for {
		totalCount++
		pageCount = int(math.Ceil(float64(totalCount) / float64(pageSize)))
		if page > pageCount {
			if "" == index.Parent {
				break
			}
			index, err = repo.store.GetIndex(index.Parent)
			if nil != err {
				return
			}
			continue
		}

		if page == pageCount {
			indexes = append(indexes, index)
		}

		if "" == index.Parent {
			break
		}
		index, err = repo.store.GetIndex(index.Parent)
		if nil != err {
			return
		}
	}

	for _, idx := range indexes {
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
		ID:       index.ID,
		Parent:   index.Parent,
		Memo:     index.Memo,
		Created:  index.Created,
		HCreated: time.UnixMilli(index.Created).Format("2006-01-02 15:04:05"),
		Size:     index.Size,
		Count:    index.Count,
		HSize:    humanize.Bytes(uint64(index.Size)),
	}
	return
}

func (repo *Repo) getInitIndex(latest *entity.Index) (ret *entity.Index, err error) {
	for {
		if "" == latest.Parent {
			ret = latest
			return
		}
		latest, err = repo.store.GetIndex(latest.Parent)
		if nil != err {
			return
		}
	}
	return
}

// getIndexes 返回 [fromID, toID) 区间内的索引。
func (repo *Repo) getIndexes(fromID, toID string) (ret []*entity.Index) {
	ret = []*entity.Index{}
	added := map[string]bool{} // 意外出现循环引用时跳出
	const max = 512            // 最大深度跳出
	var i int
	for max > i {
		index, err := repo.store.GetIndex(fromID)
		i++
		added[index.ID] = true

		// 通过内部存储的 ID 检查文件不存在的话跳过该索引
		if _, statErr := repo.store.Stat(index.ID); nil != statErr {
			fromID = index.Parent
			if "" == fromID || fromID == toID {
				return
			}
			continue
		}

		if nil != err || added[index.ID] {
			return
		}
		ret = append(ret, index)

		if index.Parent == toID || "" == index.Parent || index.ID == toID {
			return
		}
		fromID = index.Parent
	}
	return
}
