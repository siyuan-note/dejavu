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
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/88250/gulu"
	"github.com/dustin/go-humanize"
	"github.com/siyuan-note/dejavu/entity"
)

type Log struct {
	ID          string         `json:"id"`          // Hash
	Parent      string         `json:"parent"`      // 指向上一个索引
	Memo        string         `json:"memo"`        // 索引备注
	Created     int64          `json:"created"`     // 索引时间
	HCreated    string         `json:"hCreated"`    // 索引时间 "2006-01-02 15:04:05"
	Files       []*entity.File `json:"files"`       // 文件列表
	Count       int            `json:"count"`       // 文件总数
	Size        int64          `json:"size"`        // 文件总大小
	HSize       string         `json:"hSize"`       // 格式化好的文件总大小 "10.00 MB"
	Tag         string         `json:"tag"`         // 索引标记名称
	HTagUpdated string         `json:"hTagUpdated"` // 标记时间 "2006-01-02 15:04:05"
}

func (log *Log) String() string {
	data, err := gulu.JSON.MarshalJSON(log)
	if nil != err {
		return "print log [" + log.ID + "] failed"
	}
	return string(data)
}

func (repo *Repo) GetCloudRepoTagLogs(context map[string]interface{}) (ret []*Log, err error) {
	cloudTags, err := repo.cloud.GetTags()
	if nil != err {
		return
	}
	for _, tag := range cloudTags {
		index, _ := repo.store.GetIndex(tag.ID)
		if nil == index {
			_, index, err = repo.downloadCloudIndex(tag.ID, context)
			if nil != err {
				return
			}
		}

		var log *Log
		log, err = repo.getLog(index, false)
		if nil != err {
			return
		}
		log.Tag = tag.Name
		log.HTagUpdated = tag.Updated
		ret = append(ret, log)
	}
	sort.Slice(ret, func(i, j int) bool { return ret[i].Created > ret[j].Created })
	return
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
		name := entry.Name()
		data, err = os.ReadFile(filepath.Join(tags, name))
		if nil != err {
			return
		}
		info, _ := os.Stat(filepath.Join(tags, name))
		updated := info.ModTime().Format("2006-01-02 15:04:05")
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
		log, err = repo.getLog(index, true)
		if nil != err {
			return
		}
		log.Tag = name
		log.HTagUpdated = updated
		ret = append(ret, log)
	}
	sort.Slice(ret, func(i, j int) bool { return ret[i].Created > ret[j].Created })
	return
}

func (repo *Repo) GetIndexLogs(page, pageSize int) (ret []*Log, pageCount, totalCount int, err error) {
	index, err := repo.Latest()
	if nil != err {
		return
	}

	added := map[string]bool{}
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
		added[index.ID] = true
		if "" == index.Parent {
			break
		}
		index, _ = repo.store.GetIndex(index.Parent)
		if nil == index {
			break
		}
		if added[index.ID] {
			break
		}
	}

	for _, idx := range indexes {
		var log *Log
		log, err = repo.getLog(idx, true)
		if nil != err {
			return
		}
		ret = append(ret, log)
	}
	return
}

func (repo *Repo) getLog(index *entity.Index, fetchFiles bool) (ret *Log, err error) {
	var files []*entity.File
	if fetchFiles {
		files, _ = repo.getFiles(index.Files)
	}
	ret = &Log{
		ID:       index.ID,
		Parent:   index.Parent,
		Memo:     index.Memo,
		Created:  index.Created,
		HCreated: time.UnixMilli(index.Created).Format("2006-01-02 15:04:05"),
		Files:    files,
		Count:    index.Count,
		Size:     index.Size,
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
	const max = 64             // 最大深度跳出
	var i int
	for index, err := repo.store.GetIndex(fromID); max > i; i++ {
		if nil != err || added[index.ID] {
			return
		}
		if index.ID != fromID { // 意外情况：存储的 ID 和文件名不一致
			// 继续查找上一个索引
			fromID = index.Parent
			if fromID == toID || "" == fromID {
				return
			}
			continue
		}

		ret = append(ret, index)
		added[index.ID] = true
		if index.Parent == toID || "" == index.Parent || index.ID == toID {
			return
		}
		fromID = index.Parent
	}
	return
}
