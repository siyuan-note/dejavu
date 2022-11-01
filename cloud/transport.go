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

import "errors"

// Conf 用于描述云端存储服务配置信息。
type Conf struct {
	Dir      string                 // 存储目录
	UserID   string                 // 用户 ID，没有的话请传入一个定值比如 "0"
	RepoPath string                 // 本地仓库的绝对路径，如：F:\\SiYuan\\repo\\
	Extras   map[string]interface{} // 一些可能需要的附加信息

	// S3 协议所需配置
	Endpoint  string // 服务端点
	AccessKey string // Access Key
	SecretKey string // Secret Key
	Regin     string // 存储区域
	Bucket    string // 存储空间

	// 以下值非官方存储服务不必传入
	Token     string // 云端接口鉴权令牌
	LimitSize int64  // 云端存储空间限制，0 为不限制
	Server    string // 云端接口端点
}

// Cloud 描述了云端存储服务，接入云端存储服务时需要实现该接口。
type Cloud interface {

	// GetConf 用于获取配置信息。
	GetConf() *Conf

	// CreateRepo 用于创建云端仓库。
	CreateRepo(name string) (err error)

	// RemoveRepo 用于删除云端仓库。
	RemoveRepo(name string) (err error)

	// GetRepos 用于获取云端仓库列表。
	GetRepos() (repos []map[string]interface{}, size int64, err error)

	// GetLimitSize 用于获取云端存储空间限制。
	GetLimitSize() (ret int64)

	// UploadObject 用于上传对象，overwrite 参数用于指示是否覆盖已有对象。
	UploadObject(filePath string, overwrite bool) (err error)

	// DownloadObject 用于下载对象。
	DownloadObject(key string) (data []byte, err error)

	// AddTraffic 用于统计流量。
	AddTraffic(uploadBytes, downloadBytes int64)
}

type BaseCloud struct {
	*Conf
	Cloud
}

func (siyuan *SiYuan) GetLimitSize() (ret int64) {
	return siyuan.Conf.LimitSize
}

func (siyuan *SiYuan) GetConf() *Conf {
	return siyuan.Conf
}

var (
	// ErrCloudObjectNotFound 描述了云端存储服务中的对象不存在的错误。
	ErrCloudObjectNotFound = errors.New("cloud object not found")
	// ErrCloudAuthFailed 描述了云端存储服务鉴权失败的错误。
	ErrCloudAuthFailed = errors.New("cloud account auth failed")
)
