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

package cloud

import (
	"context"
	"fmt"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/88250/gulu"
	"github.com/qiniu/go-sdk/v7/client"
	"github.com/qiniu/go-sdk/v7/storage"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/httpclient"
	"github.com/siyuan-note/logging"
)

var clientInit = sync.Once{}

// SiYuan 描述了思源笔记官方云端存储服务实现。
type SiYuan struct {
	*BaseCloud
}

func NewSiYuan(baseCloud *BaseCloud) *SiYuan {
	clientInit.Do(func() {
		client.DefaultClient = client.Client{Client: httpclient.GetCloudFileClient2Min()}
		storage.DefaultClient = client.DefaultClient
	})
	return &SiYuan{BaseCloud: baseCloud}
}

func (siyuan *SiYuan) UploadObject(filePath string, overwrite bool) (err error) {
	absFilePath := filepath.Join(siyuan.Conf.RepoPath, filePath)

	key := path.Join("siyuan", siyuan.Conf.UserID, "repo", siyuan.Conf.Dir, filePath)
	keyUploadToken, scopeUploadToken, err := siyuan.requestScopeKeyUploadToken(key, overwrite)
	if nil != err {
		return
	}

	uploadToken := keyUploadToken
	if !overwrite {
		uploadToken = scopeUploadToken
	}

	region, _ := storage.GetRegionByID(storage.RIDHuadong)
	formUploader := storage.NewFormUploader(&storage.Config{UseHTTPS: true, Region: &region, UseCdnDomains: true})

	ret := storage.PutRet{}
	err = formUploader.PutFile(context.Background(), &ret, uploadToken, key, absFilePath, nil)
	if nil != err {
		if msg := fmt.Sprintf("%s", err); strings.Contains(msg, "file exists") {
			err = nil
			return
		}

		logging.LogErrorf("upload object [%s] failed: %s", absFilePath, err)
		if e, ok := err.(*client.ErrorInfo); ok {
			if 614 == e.Code {
				// file exists
				err = nil
				return
			}
			if strings.Contains(e.Err, "file exists") {
				err = nil
				return
			}

			logging.LogErrorf("error detail: %s", e.ErrorDetail())
		}

		time.Sleep(1 * time.Second)
		err = formUploader.PutFile(context.Background(), &ret, uploadToken, key, absFilePath, nil)
		if nil != err {
			if msg := fmt.Sprintf("%s", err); strings.Contains(msg, "file exists") {
				err = nil
				return
			}

			logging.LogErrorf("upload object [%s] failed: %s", absFilePath, err)
			if e, ok := err.(*client.ErrorInfo); ok {
				if 614 == e.Code {
					// file exists
					err = nil
					return
				}
				if strings.Contains(e.Err, "file exists") {
					err = nil
					return
				}

				logging.LogErrorf("error detail: %s", e.ErrorDetail())
			}
		}
		return
	}

	//logging.LogInfof("uploaded object [%s]", key)
	return
}

func (siyuan *SiYuan) DownloadObject(filePath string) (ret []byte, err error) {
	key := path.Join("siyuan", siyuan.Conf.UserID, "repo", siyuan.Conf.Dir, filePath)
	resp, err := httpclient.NewCloudFileRequest2m().Get(siyuan.Endpoint + key)
	if nil != err {
		err = fmt.Errorf("download object [%s] failed: %s", key, err)
		return
	}
	if 200 != resp.StatusCode {
		if 404 == resp.StatusCode {
			if !strings.HasSuffix(key, "/refs/latest") && !strings.HasSuffix(key, "/lock-sync") {
				logging.LogErrorf("download object [%s] failed: %s", key, ErrCloudObjectNotFound)
			}
			err = ErrCloudObjectNotFound
			return
		}
		err = fmt.Errorf("download object [%s] failed [%d]", key, resp.StatusCode)
		return
	}

	ret, err = resp.ToBytes()
	if nil != err {
		err = fmt.Errorf("download read data failed: %s", err)
		return
	}
	//logging.LogInfof("downloaded object [%s]", key)
	return
}

func (siyuan *SiYuan) RemoveObject(filePath string) (err error) {
	userId := siyuan.Conf.UserID
	dir := siyuan.Conf.Dir
	token := siyuan.Conf.Token
	server := siyuan.Conf.Server

	key := path.Join("siyuan", userId, "repo", dir, filePath)
	result := gulu.Ret.NewResult()
	request := httpclient.NewCloudRequest30s()
	resp, err := request.
		SetSuccessResult(&result).
		SetBody(map[string]string{"repo": dir, "token": token, "key": key}).
		Post(server + "/apis/siyuan/dejavu/removeRepoObject?uid=" + userId)
	if nil != err {
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = ErrCloudAuthFailed
			return
		}
		err = fmt.Errorf("remove cloud repo object failed [%d]", resp.StatusCode)
		return
	}

	if 0 != result.Code {
		err = fmt.Errorf("remove cloud repo object failed: %s", result.Msg)
		return
	}
	return
}

func (siyuan *SiYuan) GetTags() (tags []*Ref, err error) {
	token := siyuan.Conf.Token
	dir := siyuan.Conf.Dir
	userId := siyuan.Conf.UserID
	server := siyuan.Conf.Server

	result := gulu.Ret.NewResult()
	request := httpclient.NewCloudRequest30s()
	resp, err := request.
		SetSuccessResult(&result).
		SetBody(map[string]string{"repo": dir, "token": token}).
		Post(server + "/apis/siyuan/dejavu/getRepoTags?uid=" + userId)
	if nil != err {
		err = fmt.Errorf("get cloud repo tags failed: %s", err)
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = ErrCloudAuthFailed
			return
		}
		err = fmt.Errorf("get cloud repo tags failed [%d]", resp.StatusCode)
		return
	}

	if 0 != result.Code {
		err = fmt.Errorf("get cloud repo tags failed: %s", result.Msg)
		return
	}

	retData := result.Data.(map[string]interface{})
	retTags := retData["tags"].([]interface{})
	for _, retTag := range retTags {
		data, marshalErr := gulu.JSON.MarshalJSON(retTag)
		if nil != marshalErr {
			logging.LogErrorf("marshal tag failed: %s", marshalErr)
			continue
		}
		tag := &Ref{}
		if unmarshalErr := gulu.JSON.UnmarshalJSON(data, tag); nil != unmarshalErr {
			logging.LogErrorf("unmarshal tag failed: %s", unmarshalErr)
			continue
		}
		tags = append(tags, tag)
	}
	return
}

func (siyuan *SiYuan) GetIndexes(page int) (indexes []*entity.Index, pageCount, totalCount int, err error) {
	token := siyuan.Conf.Token
	dir := siyuan.Conf.Dir
	userId := siyuan.Conf.UserID
	server := siyuan.Conf.Server

	result := gulu.Ret.NewResult()
	request := httpclient.NewCloudRequest30s()
	resp, err := request.
		SetSuccessResult(&result).
		SetBody(map[string]interface{}{"repo": dir, "token": token, "page": page}).
		Post(server + "/apis/siyuan/dejavu/getRepoIndexes?uid=" + userId)
	if nil != err {
		err = fmt.Errorf("get cloud repo tags failed: %s", err)
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = ErrCloudAuthFailed
			return
		}
		err = fmt.Errorf("get cloud repo tags failed [%d]", resp.StatusCode)
		return
	}

	if 0 != result.Code {
		err = fmt.Errorf("get cloud repo tags failed: %s", result.Msg)
		return
	}

	retData := result.Data.(map[string]interface{})
	retIndexes := retData["indexes"].([]interface{})
	for _, retIndex := range retIndexes {
		data, marshalErr := gulu.JSON.MarshalJSON(retIndex)
		if nil != marshalErr {
			logging.LogErrorf("marshal index failed: %s", marshalErr)
			continue
		}
		index := &entity.Index{}
		if unmarshalErr := gulu.JSON.UnmarshalJSON(data, index); nil != unmarshalErr {
			logging.LogErrorf("unmarshal index failed: %s", unmarshalErr)
			continue
		}
		indexes = append(indexes, index)
	}

	pageCount = int(retData["pageCount"].(float64))
	totalCount = int(retData["totalCount"].(float64))
	return
}

func (siyuan *SiYuan) GetRefsFiles() (fileIDs []string, refs []*Ref, err error) {
	token := siyuan.Conf.Token
	dir := siyuan.Conf.Dir
	userId := siyuan.Conf.UserID
	server := siyuan.Conf.Server

	result := gulu.Ret.NewResult()
	request := httpclient.NewCloudFileRequest2m()
	resp, err := request.
		SetSuccessResult(&result).
		SetBody(map[string]string{"repo": dir, "token": token}).
		Post(server + "/apis/siyuan/dejavu/getRepoRefsFiles?uid=" + userId)
	if nil != err {
		err = fmt.Errorf("get cloud repo refs files failed: %s", err)
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = ErrCloudAuthFailed
			return
		}
		err = fmt.Errorf("get cloud repo refs files failed [%d]", resp.StatusCode)
		return
	}

	if 0 != result.Code {
		err = fmt.Errorf("get cloud repo refs files failed: %s", result.Msg)
		return
	}

	retData := result.Data.(map[string]interface{})
	retFiles := retData["files"].([]interface{})
	for _, retFile := range retFiles {
		fileIDs = append(fileIDs, retFile.(string))
	}
	retRefs := retData["refs"].([]interface{})
	for _, retRef := range retRefs {
		data, marshalErr := gulu.JSON.MarshalJSON(retRef)
		if nil != marshalErr {
			logging.LogErrorf("marshal ref failed: %s", marshalErr)
			continue
		}
		ref := &Ref{}
		if unmarshalErr := gulu.JSON.UnmarshalJSON(data, ref); nil != unmarshalErr {
			logging.LogErrorf("unmarshal ref failed: %s", unmarshalErr)
			continue
		}
		refs = append(refs, ref)
	}
	return
}

func (siyuan *SiYuan) GetChunks(excludeChunkIDs []string) (chunkIDs []string, err error) {
	if 1 > len(excludeChunkIDs) {
		return
	}

	token := siyuan.Conf.Token
	dir := siyuan.Conf.Dir
	userId := siyuan.Conf.UserID
	server := siyuan.Conf.Server

	result := gulu.Ret.NewResult()
	request := httpclient.NewCloudFileRequest2m()
	resp, err := request.
		SetSuccessResult(&result).
		SetBody(map[string]interface{}{"repo": dir, "token": token, "chunks": excludeChunkIDs}).
		Post(server + "/apis/siyuan/dejavu/getRepoUploadChunks?uid=" + userId)
	if nil != err {
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = ErrCloudAuthFailed
			return
		}
		err = fmt.Errorf("get cloud repo refs chunks failed [%d]", resp.StatusCode)
		return
	}

	if 0 != result.Code {
		err = fmt.Errorf("get cloud repo refs chunks failed: %s", result.Msg)
		return
	}

	retData := result.Data.(map[string]interface{})
	retChunks := retData["chunks"].([]interface{})
	for _, retChunk := range retChunks {
		chunkIDs = append(chunkIDs, retChunk.(string))
	}
	return
}

func (siyuan *SiYuan) GetStat() (stat *Stat, err error) {
	token := siyuan.Conf.Token
	dir := siyuan.Conf.Dir
	userId := siyuan.Conf.UserID
	server := siyuan.Conf.Server

	result := gulu.Ret.NewResult()
	request := httpclient.NewCloudRequest30s()
	resp, err := request.
		SetSuccessResult(&result).
		SetBody(map[string]string{"repo": dir, "token": token}).
		Post(server + "/apis/siyuan/dejavu/getRepoStat?uid=" + userId)
	if nil != err {
		err = fmt.Errorf("get cloud repo stat failed: %s", err)
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = ErrCloudAuthFailed
			return
		}
		err = fmt.Errorf("get cloud repo stat failed [%d]", resp.StatusCode)
		return
	}

	if 0 != result.Code {
		err = fmt.Errorf("get cloud repo stat failed: %s", result.Msg)
		return
	}

	data, marshalErr := gulu.JSON.MarshalJSON(result.Data)
	if nil != marshalErr {
		err = fmt.Errorf("marshal stat failed: %s", marshalErr)
		return
	}
	stat = &Stat{}
	if unmarshalErr := gulu.JSON.UnmarshalJSON(data, stat); nil != unmarshalErr {
		err = fmt.Errorf("unmarshal stat failed: %s", unmarshalErr)
		return
	}
	return
}

func (siyuan *SiYuan) AddTraffic(traffic *Traffic) {
	token := siyuan.Conf.Token
	server := siyuan.Conf.Server

	request := httpclient.NewCloudRequest30s()
	resp, err := request.
		SetBody(map[string]interface{}{
			"token":         token,
			"uploadBytes":   traffic.UploadBytes,
			"downloadBytes": traffic.DownloadBytes,
			"apiGet":        traffic.APIGet,
			"apiPut":        traffic.APIPut,
		}).
		Post(server + "/apis/siyuan/dejavu/addTraffic")
	if nil != err {
		logging.LogErrorf("add traffic failed: %s", err)
		return
	}

	if 200 != resp.StatusCode {
		logging.LogErrorf("add traffic failed: %d", resp.StatusCode)
		return
	}
	return
}

func (siyuan *SiYuan) RemoveRepo(name string) (err error) {
	token := siyuan.Conf.Token
	server := siyuan.Conf.Server

	request := httpclient.NewCloudFileRequest2m()
	resp, err := request.
		SetBody(map[string]string{"name": name, "token": token}).
		Post(server + "/apis/siyuan/dejavu/removeRepo")
	if nil != err {
		err = fmt.Errorf("remove cloud repo failed: %s", err)
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = ErrCloudAuthFailed
			return
		}
		err = fmt.Errorf("remove cloud repo failed [%d]", resp.StatusCode)
		return
	}
	return
}

func (siyuan *SiYuan) CreateRepo(name string) (err error) {
	token := siyuan.Conf.Token
	server := siyuan.Conf.Server

	result := map[string]interface{}{}
	request := httpclient.NewCloudRequest30s()
	resp, err := request.
		SetSuccessResult(&result).
		SetBody(map[string]string{"name": name, "token": token}).
		Post(server + "/apis/siyuan/dejavu/createRepo")
	if nil != err {
		err = fmt.Errorf("create cloud repo failed: %s", err)
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = ErrCloudAuthFailed
			return
		}
		err = fmt.Errorf("create cloud repo failed [%d]", resp.StatusCode)
		return
	}

	code := result["code"].(float64)
	if 0 != code {
		err = fmt.Errorf("create cloud repo failed: %s", result["msg"])
		return
	}
	return
}

func (siyuan *SiYuan) GetRepos() (repos []*Repo, size int64, err error) {
	token := siyuan.Conf.Token
	server := siyuan.Conf.Server
	userId := siyuan.Conf.UserID

	result := map[string]interface{}{}
	request := httpclient.NewCloudRequest30s()
	resp, err := request.
		SetBody(map[string]interface{}{"token": token}).
		SetSuccessResult(&result).
		Post(server + "/apis/siyuan/dejavu/getRepos?uid=" + userId)
	if nil != err {
		err = fmt.Errorf("get cloud repos failed: %s", err)
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = ErrCloudAuthFailed
			return
		}
		err = fmt.Errorf("request cloud repo list failed [%d]", resp.StatusCode)
		return
	}

	code := result["code"].(float64)
	if 0 != code {
		err = fmt.Errorf("request cloud repo list failed: %s", result["msg"].(string))
		return
	}

	retData := result["data"].(map[string]interface{})
	retRepos := retData["repos"].([]interface{})
	for _, d := range retRepos {
		data, marshalErr := gulu.JSON.MarshalJSON(d)
		if nil != marshalErr {
			logging.LogErrorf("marshal repo failed: %s", marshalErr)
			continue
		}
		repo := &Repo{}
		if unmarshalErr := gulu.JSON.UnmarshalJSON(data, repo); nil != unmarshalErr {
			logging.LogErrorf("unmarshal repo failed: %s", unmarshalErr)
			continue
		}

		repos = append(repos, repo)
	}
	if 1 > len(repos) {
		repos = []*Repo{}
	}
	sort.Slice(repos, func(i, j int) bool { return repos[i].Name < repos[j].Name })
	size = int64(retData["size"].(float64))
	return
}

type UploadToken struct {
	key, token string
	expired    int64
}

var (
	keyUploadTokenMap   = map[string]*UploadToken{}
	scopeUploadTokenMap = map[string]*UploadToken{}
	uploadTokenMapLock  = &sync.Mutex{}
)

func (siyuan *SiYuan) requestScopeKeyUploadToken(key string, overwrite bool) (keyToken, scopeToken string, err error) {
	userId := siyuan.Conf.UserID
	now := time.Now().UnixMilli()
	keyPrefix := path.Join("siyuan", userId)

	uploadTokenMapLock.Lock()
	cachedKeyToken := keyUploadTokenMap[key]
	if nil != cachedKeyToken {
		if now < cachedKeyToken.expired {
			keyToken = cachedKeyToken.token
		} else {
			delete(keyUploadTokenMap, key)
		}
	}
	if overwrite && "" != keyToken {
		uploadTokenMapLock.Unlock()
		return
	}

	cachedScopeToken := scopeUploadTokenMap[keyPrefix]
	if nil != cachedScopeToken {
		if now < cachedScopeToken.expired {
			scopeToken = cachedScopeToken.token
		} else {
			delete(scopeUploadTokenMap, keyPrefix)
		}
	}
	if !overwrite && "" != scopeToken {
		uploadTokenMapLock.Unlock()
		return
	}
	uploadTokenMapLock.Unlock()

	token := siyuan.Conf.Token
	server := siyuan.Conf.Server
	var result map[string]interface{}
	req := httpclient.NewCloudRequest30s().SetSuccessResult(&result)
	req.SetBody(map[string]interface{}{
		"token":     token,
		"key":       key,
		"keyPrefix": keyPrefix,
		"time":      now, // 数据同步加入系统时间校验 https://github.com/siyuan-note/siyuan/issues/7669
	})
	resp, err := req.Post(server + "/apis/siyuan/dejavu/getRepoScopeKeyUploadToken?uid=" + userId)
	if nil != err {
		err = fmt.Errorf("request repo upload token failed: %s", err)
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = ErrCloudAuthFailed
			return
		}
		err = fmt.Errorf("request repo upload token failed [%d]", resp.StatusCode)
		return
	}

	code := result["code"].(float64)
	if 0 != code {
		msg := result["msg"].(string)
		err = fmt.Errorf("request repo upload token failed: %s", msg)
		if 1 == code { // 系统时间不一致
			err = ErrSystemTimeIncorrect
		}
		return
	}

	resultData := result["data"].(map[string]interface{})
	keyToken = resultData["keyToken"].(string)
	scopeToken = resultData["scopeToken"].(string)
	expired := now + 1000*60*60*24 - 60*1000
	uploadTokenMapLock.Lock()
	keyUploadTokenMap[key] = &UploadToken{
		key:     key,
		token:   keyToken,
		expired: expired,
	}
	scopeUploadTokenMap[keyPrefix] = &UploadToken{
		key:     keyPrefix,
		token:   scopeToken,
		expired: expired,
	}
	uploadTokenMapLock.Unlock()
	return
}
