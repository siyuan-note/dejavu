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
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"time"

	"github.com/88250/gulu"
	"github.com/qiniu/go-sdk/v7/storage"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/httpclient"
)

func (repo *Repo) Sync(cloudDir, userId, token, proxyURL, server string) (err error) {
	// TODO: 先打快照

	latest, err := repo.Latest()
	if nil != err {
		return
	}

	latestSync, err := repo.latestSync()
	if nil != err {
		return
	}

	if latest.ID == latestSync.ID {
		return
	}

	localIndexes, err := repo.getIndexes(latest.ID, latestSync.ID)
	if nil != err {
		return
	}

	// 从云端获取索引列表
	cloudIndexes, err := repo.downloadCloudIndexes(cloudDir, latestSync.ID, userId, token, proxyURL, server)
	if nil != err {
		return
	}

	// 从索引中得到去重后的文件列表
	cloudFileIDs := repo.getFileIDs(cloudIndexes)

	// 计算本地缺失的文件
	fetchFiles, err := repo.localNotFoundFiles(cloudFileIDs)
	if nil != err {
		return
	}

	// 从云端获取文件列表
	var files []*entity.File
	for _, fileID := range fetchFiles {
		var file *entity.File
		file, err = repo.downloadCloudFile(cloudDir, fileID, userId, token, proxyURL, server)
		if nil != err {
			return
		}
		files = append(files, file)
	}

	// 从文件列表中得到去重后的分块列表
	cloudChunkIDs := repo.getChunks(files)

	// 计算本地缺失的分块
	fetchChunks, err := repo.localNotFoundChunks(cloudChunkIDs)
	if nil != err {
		return
	}

	// 计算待上传云端的本地变更文件
	upsertFiles, err := repo.localUpsertFiles(latest, latestSync)
	if nil != err {
		return
	}

	// 计算待上传云端的分块
	upsertChunkIDs, err := repo.localUpsertChunkIDs(upsertFiles, cloudChunkIDs)
	if nil != err {
		return
	}

	// 上传分块
	for _, upsertChunkID := range upsertChunkIDs {
		err = repo.uploadLocalObject(cloudDir, upsertChunkID, userId, token, proxyURL, server)
		if nil != err {
			return
		}
	}

	// 上传文件
	for _, upsertFile := range upsertFiles {
		err = repo.uploadLocalObject(cloudDir, upsertFile.ID, userId, token, proxyURL, server)
		if nil != err {
			return
		}
	}

	var chunks []*entity.Chunk
	// 从云端获取分块
	for _, chunkID := range fetchChunks {
		var chunk *entity.Chunk
		chunk, err = repo.downloadCloudChunk(cloudDir, chunkID, userId, token, proxyURL, server)
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
			if "" != index.Parent {
				index.Parent = latest.ID
			}
		}
		err = repo.store.PutIndex(index)
		if nil != err {
			return
		}
	}

	// 上传索引
	err = repo.uploadIndexes(cloudDir, allIndexes, userId, token, proxyURL, server)
	if nil != err {
		return
	}

	latest = allIndexes[0]

	// 更新本地 latest
	err = repo.UpdateLatest(latest.ID)
	if nil != err {
		return
	}

	// 更新云端 latest
	err = repo.UpdateCloudLatest(cloudDir, userId, token, proxyURL, server)
	if nil != err {
		return
	}

	// 更新本地同步点
	err = repo.UpdateLatestSync(latest.ID)
	return
}

func (repo *Repo) uploadIndexes(cloudDir string, indexes []*entity.Index, userId, token, proxyURL, server string) (err error) {
	for _, index := range indexes {
		err = repo.uploadLocalIndex(cloudDir, index.ID, userId, token, proxyURL, server)
		if nil != err {
			return
		}
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
	ret = gulu.Str.RemoveDuplicatedElem(ret)
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
	ret = gulu.Str.RemoveDuplicatedElem(ret)
	return
}

func (repo *Repo) getChunks(files []*entity.File) (chunkIDs []string) {
	for _, file := range files {
		chunkIDs = append(chunkIDs, file.Chunks...)
	}
	chunkIDs = gulu.Str.RemoveDuplicatedElem(chunkIDs)
	return
}

func (repo *Repo) getFileIDs(indexes []*entity.Index) (fileIDs []string) {
	for _, index := range indexes {
		fileIDs = append(fileIDs, index.Files...)
	}
	fileIDs = gulu.Str.RemoveDuplicatedElem(fileIDs)
	return
}

func (repo *Repo) localUpsertChunkIDs(localFiles []*entity.File, cloudChunkIDs []string) (ret []string, err error) {
	chunks := map[string]bool{}
	for _, file := range localFiles {
		for _, chunkID := range file.Chunks {
			chunks[chunkID] = true
		}
	}

	for _, cloudChunkID := range cloudChunkIDs {
		delete(chunks, cloudChunkID)
	}

	for chunkID := range chunks {
		ret = append(ret, chunkID)
	}
	return
}

func (repo *Repo) localUpsertFiles(latest, latestSync *entity.Index) (ret []*entity.File, err error) {
	files := map[string]bool{}
	for {
		for _, file := range latest.Files {
			files[file] = true
		}

		if latest.Parent == latestSync.ID || "" == latest.Parent {
			break
		}

		latest, err = repo.store.GetIndex(latest.Parent)
		if nil != err {
			return
		}
	}

	for _, file := range latestSync.Files {
		delete(files, file)
	}

	for fileID := range files {
		var file *entity.File
		file, err = repo.store.GetFile(fileID)
		if nil != err {
			return
		}

		ret = append(ret, file)
	}
	return
}

func (repo *Repo) UpdateLatestSync(id string) (err error) {
	refs := filepath.Join(repo.Path, "refs")
	err = os.MkdirAll(refs, 0755)
	if nil != err {
		return
	}
	err = gulu.File.WriteFileSafer(filepath.Join(refs, "latest-sync"), []byte(id), 0644)
	return
}

func (repo *Repo) latestSync() (ret *entity.Index, err error) {
	latestSync := filepath.Join(repo.Path, "refs", "latest-sync")
	if !gulu.File.IsExist(latestSync) {
		ret = &entity.Index{} // 构造一个空的索引表示没有同步点
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

func (repo *Repo) UpdateCloudLatest(repoDir, userId, token, proxyURL, server string) (err error) {
	uploadToken, err := repo.requestLatestUploadToken(repoDir, userId, token, proxyURL, server, 40)
	if nil != err {
		return
	}

	key := path.Join("siyuan", userId, "repo", repoDir, "refs", "latest")
	formUploader := storage.NewFormUploader(&storage.Config{UseHTTPS: true})
	ret := storage.PutRet{}
	filePath := filepath.Join(repo.Path, "refs", "latest")
	err = formUploader.PutFile(context.Background(), &ret, uploadToken, key, filePath, nil)
	if nil != err {
		time.Sleep(3 * time.Second)
		err = formUploader.PutFile(context.Background(), &ret, uploadToken, key, filePath, nil)
		if nil != err {
			return
		}
	}
	return
}

func (repo *Repo) uploadLocalIndex(repoDir string, id, userId, token, proxyURL, server string) (err error) {
	_, file := repo.store.IndexAbsPath(id)
	info, statErr := os.Stat(file)
	if nil != statErr {
		err = statErr
		return
	}

	uploadToken, err := repo.requestIndexUploadToken(repoDir, id, userId, token, proxyURL, server, info.Size())
	if nil != err {
		return
	}

	key := path.Join("siyuan", userId, "repo", repoDir, "indexes", id)
	formUploader := storage.NewFormUploader(&storage.Config{UseHTTPS: true})
	ret := storage.PutRet{}
	filePath := filepath.Join(repo.Path, "indexes", id)
	err = formUploader.PutFile(context.Background(), &ret, uploadToken, key, filePath, nil)
	if nil != err {
		time.Sleep(3 * time.Second)
		err = formUploader.PutFile(context.Background(), &ret, uploadToken, key, filePath, nil)
		if nil != err {
			return
		}
	}
	return
}

func (repo *Repo) uploadLocalObject(repoDir, id, userId, token, proxyURL, server string) (err error) {
	info, statErr := repo.store.Stat(id)
	if nil != statErr {
		err = statErr
		return
	}

	uploadToken, err := repo.requestObjectUploadToken(repoDir, id, userId, token, proxyURL, server, info.Size())
	if nil != err {
		return
	}

	key := path.Join("siyuan", userId, "repo", repoDir, "objects", id[:2], id[2:])
	formUploader := storage.NewFormUploader(&storage.Config{UseHTTPS: true})
	ret := storage.PutRet{}
	filePath := filepath.Join(repo.Path, "objects", id[:2], id[2:])
	err = formUploader.PutFile(context.Background(), &ret, uploadToken, key, filePath, nil)
	if nil != err {
		time.Sleep(3 * time.Second)
		err = formUploader.PutFile(context.Background(), &ret, uploadToken, key, filePath, nil)
		if nil != err {
			return
		}
	}
	return
}

func (repo *Repo) requestLatestUploadToken(repoDir, userId, token, proxyURL, server string, length int64) (ret string, err error) {
	// 因为需要指定 key，所以每次上传文件都必须在云端生成 Token，否则有安全隐患

	var result map[string]interface{}
	req := httpclient.NewCloudRequest(proxyURL).
		SetResult(&result)
	req.SetBody(map[string]interface{}{
		"token":  token,
		"repo":   repoDir,
		"length": length})
	resp, err := req.Post(server + "/apis/siyuan/dejavu/getRepoLatestUploadToken?uid=" + userId)
	if nil != err {
		err = errors.New("request object upload token failed: " + err.Error())
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = errors.New("account authentication failed, please login again")
			return
		}
		err = errors.New("request index upload token failed")
		return
	}

	code := result["code"].(float64)
	if 0 != code {
		err = errors.New("request index upload token failed: " + result["msg"].(string))
		return
	}

	resultData := result["data"].(map[string]interface{})
	ret = resultData["token"].(string)
	return
}

func (repo *Repo) requestIndexUploadToken(repoDir, id, userId, token, proxyURL, server string, length int64) (ret string, err error) {
	// 因为需要指定 key，所以每次上传文件都必须在云端生成 Token，否则有安全隐患

	var result map[string]interface{}
	req := httpclient.NewCloudRequest(proxyURL).
		SetResult(&result)
	req.SetBody(map[string]interface{}{
		"token":  token,
		"repo":   repoDir,
		"id":     id,
		"length": length})
	resp, err := req.Post(server + "/apis/siyuan/dejavu/getRepoIndexUploadToken?uid=" + userId)
	if nil != err {
		err = errors.New("request object upload token failed: " + err.Error())
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = errors.New("account authentication failed, please login again")
			return
		}
		err = errors.New("request index upload token failed")
		return
	}

	code := result["code"].(float64)
	if 0 != code {
		err = errors.New("request index upload token failed: " + result["msg"].(string))
		return
	}

	resultData := result["data"].(map[string]interface{})
	ret = resultData["token"].(string)
	return
}

func (repo *Repo) requestObjectUploadToken(repoDir, id, userId, token, proxyURL, server string, length int64) (ret string, err error) {
	// 因为需要指定 key，所以每次上传文件都必须在云端生成 Token，否则有安全隐患

	var result map[string]interface{}
	req := httpclient.NewCloudRequest(proxyURL).
		SetResult(&result)
	req.SetBody(map[string]interface{}{
		"token":  token,
		"repo":   repoDir,
		"id":     id,
		"length": length})
	resp, err := req.Post(server + "/apis/siyuan/dejavu/getRepoObjectUploadToken?uid=" + userId)
	if nil != err {
		err = errors.New("request object upload token failed: " + err.Error())
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = errors.New("account authentication failed, please login again")
			return
		}
		err = errors.New("request object upload token failed")
		return
	}

	code := result["code"].(float64)
	if 0 != code {
		err = errors.New("request object upload token failed: " + result["msg"].(string))
		return
	}

	resultData := result["data"].(map[string]interface{})
	ret = resultData["token"].(string)
	return
}

func (repo *Repo) downloadCloudChunk(repoDir, id, userId, token, proxyURL, server string) (ret *entity.Chunk, err error) {
	data, err := repo.downloadCloudObject(repoDir, id, userId, token, proxyURL, server)
	if nil != err {
		return
	}
	ret = &entity.Chunk{}
	err = gulu.JSON.UnmarshalJSON(data, ret)
	return
}

func (repo *Repo) downloadCloudFile(repoDir, id, userId, token, proxyURL, server string) (ret *entity.File, err error) {
	data, err := repo.downloadCloudObject(repoDir, id, userId, token, proxyURL, server)
	if nil != err {
		return
	}
	ret = &entity.File{}
	err = gulu.JSON.UnmarshalJSON(data, ret)
	return
}

func (repo *Repo) downloadCloudObject(repoDir, id, userId, token, proxyURL, server string) (ret []byte, err error) {
	var result map[string]interface{}
	resp, err := httpclient.NewCloudRequest(proxyURL).
		SetResult(&result).
		SetBody(map[string]interface{}{"token": token, "repo": repoDir, "id": id}).
		Post(server + "/apis/siyuan/dejavu/getRepoObjectURL?uid=" + userId)
	if nil != err {
		err = errors.New("request object url failed: " + err.Error())
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
		err = errors.New("request object url failed: " + result["msg"].(string))
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

func (repo *Repo) downloadCloudIndexes(repoDir, latestSync, userId, token, proxyURL, server string) (indexes []*entity.Index, err error) {
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
		err = errors.New("request object url failed: " + result["msg"].(string))
		return
	}

	data := result["data"].(map[string]interface{})
	dataIndexes := data["indexes"].([]interface{})
	bytes, err := gulu.JSON.MarshalJSON(dataIndexes)
	if nil != err {
		return
	}
	err = gulu.JSON.UnmarshalJSON(bytes, &indexes)
	return
}
