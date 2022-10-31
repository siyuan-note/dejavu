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

package transport

import (
	"context"
	"fmt"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/qiniu/go-sdk/v7/client"
	"github.com/qiniu/go-sdk/v7/storage"
	"github.com/siyuan-note/httpclient"
	"github.com/siyuan-note/logging"
)

// SiYuan 描述了思源笔记官方云端存储实现。
type SiYuan struct {
	*Conf
	Transport
}

func (siyuan *SiYuan) UploadObject(filePath string, overwrite bool) (err error) {
	absFilePath := filepath.Join(siyuan.Conf.RepoPath, filePath)

	key := path.Join("siyuan", siyuan.Conf.UserID, "repo", siyuan.Conf.Dir, filePath)
	keyUploadToken, scopeUploadToken, err := siyuan.requestScopeKeyUploadToken(key)
	if nil != err {
		return
	}

	uploadToken := keyUploadToken
	if !overwrite {
		uploadToken = scopeUploadToken
	}

	formUploader := storage.NewFormUploader(&storage.Config{UseHTTPS: true})
	ret := storage.PutRet{}
	err = formUploader.PutFile(context.Background(), &ret, uploadToken, key, absFilePath, nil)
	if nil != err {
		if e, ok := err.(*client.ErrorInfo); ok && 614 == e.Code {
			// file exists
			//logging.LogWarnf("upload object [%s] exists: %s", absFilePath, err)
			err = nil
			return
		}
		time.Sleep(3 * time.Second)
		err = formUploader.PutFile(context.Background(), &ret, uploadToken, key, absFilePath, nil)
		if nil != err {
			logging.LogErrorf("upload object [%s] failed: %s", absFilePath, err)
			return
		}
	}
	//logging.LogInfof("uploaded object [%s]", key)
	return
}

func (siyuan *SiYuan) DownloadObject(key string) (ret []byte, err error) {
	resp, err := httpclient.NewCloudFileRequest15s().Get("https://siyuan-data.b3logfile.com/" + key)
	if nil != err {
		err = fmt.Errorf("download object [%s] failed: %s", key, err)
		return
	}
	if 200 != resp.StatusCode {
		if 404 == resp.StatusCode {
			if !strings.HasSuffix(key, "/refs/latest") {
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

func (siyuan *SiYuan) GetConf() *Conf {
	return siyuan.Conf
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

func (siyuan *SiYuan) requestScopeKeyUploadToken(key string) (keyToken, scopeToken string, err error) {
	userId := siyuan.Conf.UserID
	now := time.Now().UnixMilli()
	keyPrefix := path.Join("siyuan", userId)

	uploadTokenMapLock.Lock()
	cachedKeyToken := keyUploadTokenMap[key]
	cachedScopeToken := scopeUploadTokenMap[keyPrefix]
	if nil != cachedScopeToken && nil != cachedKeyToken {
		if now < cachedKeyToken.expired && now < cachedScopeToken.expired {
			keyToken = cachedKeyToken.token
			scopeToken = cachedScopeToken.token
			uploadTokenMapLock.Unlock()
			return
		}
		delete(keyUploadTokenMap, key)
		delete(scopeUploadTokenMap, keyPrefix)
	}
	uploadTokenMapLock.Unlock()

	token := siyuan.Conf.Token
	server := siyuan.Conf.Server
	var result map[string]interface{}
	req := httpclient.NewCloudRequest().SetResult(&result)
	req.SetBody(map[string]interface{}{
		"token":     token,
		"key":       key,
		"keyPrefix": keyPrefix,
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
		err = fmt.Errorf("request repo upload token failed: %s", result["msg"].(string))
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
