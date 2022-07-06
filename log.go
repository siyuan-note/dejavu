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
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/88250/gulu"
	"github.com/dustin/go-humanize"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/httpclient"
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

func (repo *Repo) RemoveCloudRepoTag(tag string, cloudInfo *CloudInfo, context map[string]interface{}) (err error) {
	result := gulu.Ret.NewResult()
	request := httpclient.NewCloudRequest(cloudInfo.ProxyURL)
	key := path.Join("siyuan", cloudInfo.UserID, "repo", cloudInfo.Dir, "refs", "tags", tag)
	resp, err := request.
		SetResult(&result).
		SetBody(map[string]string{"repo": cloudInfo.Dir, "token": cloudInfo.Token, "key": key}).
		Post(cloudInfo.Server + "/apis/siyuan/dejavu/removeRepoObject?uid=" + cloudInfo.UserID)
	if nil != err {
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = ErrCloudAuthFailed
			return
		}
		err = errors.New(fmt.Sprintf("remove cloud repo tag failed [%d]", resp.StatusCode))
		return
	}

	if 0 != result.Code {
		err = errors.New(fmt.Sprintf("remove cloud repo tag failed: %s", result.Msg))
		return
	}
	return
}

func (repo *Repo) GetCloudRepoTagLogs(cloudInfo *CloudInfo, context map[string]interface{}) (ret []*Log, err error) {
	cloudTags, err := repo.GetCloudRepoTags(cloudInfo)
	if nil != err {
		return
	}
	for _, tag := range cloudTags {
		id := tag["id"].(string)
		var index *entity.Index
		_, index, err = repo.downloadCloudIndex(id, cloudInfo, context)
		if nil != err {
			return
		}

		name := tag["name"].(string)
		updated := tag["updated"].(string)
		var log *Log
		log, err = repo.getLog(index)
		if nil != err {
			return
		}
		log.Tag = name
		log.HTagUpdated = updated
		ret = append(ret, log)
	}
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
		log, err = repo.getLog(index)
		if nil != err {
			return
		}
		log.Tag = name
		log.HTagUpdated = updated
		ret = append(ret, log)
	}
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
