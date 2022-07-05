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

	"github.com/88250/gulu"
	"github.com/siyuan-note/httpclient"
)

func GetCloudRepoRefsFiles(repo string, cloudInfo *CloudInfo) (files []string, err error) {
	result := gulu.Ret.NewResult()
	request := httpclient.NewCloudRequest(cloudInfo.ProxyURL)
	resp, err := request.
		SetResult(&result).
		SetBody(map[string]string{"repo": repo, "token": cloudInfo.Token}).
		Post(cloudInfo.Server + "/apis/siyuan/dejavu/getRepoRefsFiles?uid=" + cloudInfo.UserID)
	if nil != err {
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = ErrAuthFailed
			return
		}
		err = errors.New(fmt.Sprintf("get cloud repo refs files failed [%d]", resp.StatusCode))
		return
	}

	if 0 != result.Code {
		err = errors.New(fmt.Sprintf("get cloud repo refs files failed: %s", result.Msg))
		return
	}

	retData := result.Data.(map[string]interface{})
	retFiles := retData["files"].([]interface{})
	for _, retFile := range retFiles {
		files = append(files, retFile.(string))
	}
	return
}

func GetCloudRepoTags(repo string, cloudInfo *CloudInfo) (tags []map[string]interface{}, err error) {
	result := gulu.Ret.NewResult()
	request := httpclient.NewCloudRequest(cloudInfo.ProxyURL)
	resp, err := request.
		SetResult(&result).
		SetBody(map[string]string{"repo": repo, "token": cloudInfo.Token}).
		Post(cloudInfo.Server + "/apis/siyuan/dejavu/getRepoTags?uid=" + cloudInfo.UserID)
	if nil != err {
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = ErrAuthFailed
			return
		}
		err = errors.New(fmt.Sprintf("get cloud repo tags failed [%d]", resp.StatusCode))
		return
	}

	if 0 != result.Code {
		err = errors.New(fmt.Sprintf("get cloud repo tags failed: %s", result.Msg))
		return
	}

	retData := result.Data.(map[string]interface{})
	retTags := retData["tags"].([]interface{})
	for _, retTag := range retTags {
		tags = append(tags, retTag.(map[string]interface{}))
	}
	return
}
