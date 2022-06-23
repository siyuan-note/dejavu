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
	"os"
	"path/filepath"
	"sort"

	"github.com/88250/gulu"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/dejavu/util"
	"github.com/siyuan-note/httpclient"
)

func (repo *Repo) Sync(cloudDir, userId, token, proxyURL, server string) (err error) {
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

	// 从云端获取索引列表
	cloudIndexes, err := repo.requestCloudIndexes(cloudDir, latestSync.ID, userId, token, proxyURL, server)

	// 从索引中得到去重后的文件列表
	cloudFiles, err := repo.getFiles(cloudIndexes)
	if nil != err {
		return
	}

	fetchFiles, err := repo.localNotFoundFiles(cloudFiles)
	if nil != err {
		return
	}

	// 从云端获取文件列表
	var files []*entity.File
	for _, fileID := range fetchFiles {
		var file *entity.File
		file, err = repo.requestCloudFile(cloudDir, fileID, userId, token, proxyURL, server)
		if nil != err {
			return
		}
		files = append(files, file)
	}

	// 计算缺失的分块
	var cloudChunkIDs []string
	for _, file := range files {
		cloudChunkIDs = append(cloudChunkIDs, file.Chunks...)
	}
	cloudChunkIDs = util.RemoveDuplicatedElem(cloudChunkIDs)
	fetchChunks, err := repo.localNotFoundChunks(cloudChunkIDs)
	if nil != err {
		return
	}

	var chunks []*entity.Chunk
	// 从云端获取分块
	for _, chunkID := range fetchChunks {
		var chunk *entity.Chunk
		chunk, err = repo.requestCloudChunk(cloudDir, chunkID, userId, token, proxyURL, server)
		if nil != err {
			return
		}
		chunks = append(chunks, chunk)
	}

	// 分库入库
	for _, chunk := range chunks {
		if err = repo.store.PutChunk(chunk); nil != err {
			return
		}
	}

	// 文件入库
	for _, file := range files {
		if err = repo.store.PutFile(file); nil != err {
			return
		}
	}

	// 合并云端和本地索引
	var allIndexes []*entity.Index
	allIndexes = append(allIndexes, localIndexes...)
	allIndexes = append(allIndexes, cloudIndexes...)

	// 按索引时间排序
	sort.Slice(allIndexes, func(i, j int) bool {
		return allIndexes[i].Created >= allIndexes[j].Created
	})

	// 重新排列索引入库
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

	// 更新 latest
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
	ret = util.RemoveDuplicatedElem(ret)
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
	ret = util.RemoveDuplicatedElem(ret)
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

func (repo *Repo) requestCloudChunk(repoDir, id, userId, token, proxyURL, server string) (ret *entity.Chunk, err error) {
	data, err := repo.requestCloudObjects(repoDir, id, userId, token, proxyURL, server)
	if nil != err {
		return
	}
	ret = &entity.Chunk{}
	err = gulu.JSON.UnmarshalJSON(data, ret)
	return
}

func (repo *Repo) requestCloudFile(repoDir, id, userId, token, proxyURL, server string) (ret *entity.File, err error) {
	data, err := repo.requestCloudObjects(repoDir, id, userId, token, proxyURL, server)
	if nil != err {
		return
	}
	ret = &entity.File{}
	err = gulu.JSON.UnmarshalJSON(data, ret)
	return
}

func (repo *Repo) requestCloudObjects(repoDir, id, userId, token, proxyURL, server string) (ret []byte, err error) {
	var result map[string]interface{}
	resp, err := httpclient.NewCloudRequest(proxyURL).
		SetResult(&result).
		SetBody(map[string]interface{}{"token": token, "repo": repoDir, "id": id}).
		Post(server + "/apis/siyuan/dejavu/getRepoObject?uid=" + userId)
	if nil != err {
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = errors.New("account authentication failed, please login again")
			return
		}
		err = errors.New("request object url failed")
		return
	}

	code := result["code"].(float64)
	if 0 != code {
		err = errors.New(result["msg"].(string))
		return
	}

	resultData := result["data"].(map[string]interface{})
	downloadURL := resultData["url"].(string)
	resp, err = httpclient.NewCloudFileRequest15s(proxyURL).Get(downloadURL)
	if nil != err {
		err = errors.New("download object failed")
		return
	}
	if 200 != resp.StatusCode {
		err = errors.New(fmt.Sprintf("download object failed [%d]", resp.StatusCode))
		if 404 == resp.StatusCode {
			err = errors.New("not found object")
		}
		return
	}

	ret, err = resp.ToBytes()
	if nil != err {
		err = errors.New("download read data failed")
		return
	}

	ret, err = repo.store.decodeData(ret)
	if nil != err {
		return
	}
	return
}

func (repo *Repo) requestCloudIndexes(repoDir, latestSync, userId, token, proxyURL, server string) (indexes []*entity.Index, err error) {
	var result map[string]interface{}
	resp, err := httpclient.NewCloudRequest(proxyURL).
		SetResult(&result).
		SetBody(map[string]interface{}{"token": token, "repo": repoDir, "latestSync": latestSync}).
		Post(server + "/apis/siyuan/dejavu/getRepoIndexes?uid=" + userId)
	if nil != err {
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = errors.New("account authentication failed, please login again")
			return
		}
		err = errors.New("request repo index failed")
		return
	}

	code := result["code"].(float64)
	if 0 != code {
		err = errors.New(result["msg"].(string))
		return
	}

	data := result["data"].(map[string]interface{})
	bytes, err := gulu.JSON.MarshalJSON(data)
	if nil != err {
		return
	}
	err = gulu.JSON.UnmarshalJSON(bytes, &indexes)
	return
}
