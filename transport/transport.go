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

import "errors"

// Conf 用于描述云端存储服务配置信息。
type Conf struct {
	Endpoint  string // 云端存储服务接口端点
	AccessKey string // AccessKey
	SecretKey string // SecretKey
	Regin     string // 存储区域
	Bucket    string // 存储空间

	Dir      string // 存储目录
	UserID   string // 用户 ID，没有的话请传入一个定值比如 "0"
	RepoPath string // 本地仓库的绝对路径，如：F:\\SiYuan\\repo\\

	Extras map[string]interface{} // 一些可能需要的附加信息

	// 以下值非官方存储服务不必传入
	Token     string // 云端接口鉴权令牌
	LimitSize int64  // 云端存储空间限制，0 为不限制
	Server    string // 云端接口端点
}

// Transport 描述了传输数据的方法，接入云端存储服务时必须实现该接口。
type Transport interface {

	// GetConf 用于获取配置信息。
	GetConf() *Conf

	// UploadObject 用于上传对象。
	UploadObject(filePath string) (err error)

	// DownloadObject 用于下载对象。
	DownloadObject(key string) (data []byte, err error)
}

// ErrCloudObjectNotFound 描述了云端存储服务中的对象不存在的错误。
var ErrCloudObjectNotFound = errors.New("cloud object not found")
