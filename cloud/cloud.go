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
	"errors"
	"strings"
	"unicode/utf8"
)

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
	Region    string // 存储区域
	Bucket    string // 存储空间

	// 以下值非官方存储服务不必传入
	Token         string // 云端接口鉴权令牌
	AvailableSize int64  // 云端存储可用空间字节数
	Server        string // 云端接口端点
}

// Cloud 描述了云端存储服务，接入云端存储服务时需要实现该接口。
type Cloud interface {

	// GetConf 用于获取配置信息。
	GetConf() *Conf

	// CreateRepo 用于创建名称为 name 的云端仓库。
	CreateRepo(name string) (err error)

	// RemoveRepo 用于删除云端仓库。
	RemoveRepo(name string) (err error)

	// GetRepos 用于获取云端仓库列表 repos，size 为仓库总大小字节数。
	GetRepos() (repos []*Repo, size int64, err error)

	// GetAvailableSize 用于获取云端存储可用空间字节数。
	GetAvailableSize() (size int64)

	// UploadObject 用于上传对象，overwrite 参数用于指示是否覆盖已有对象。
	UploadObject(filePath string, overwrite bool) (err error)

	// DownloadObject 用于下载对象数据 data。
	DownloadObject(key string) (data []byte, err error)

	// RemoveObject 用于删除对象。
	RemoveObject(key string) (err error)

	// GetTags 用于获取快照标记列表。
	GetTags() (tags []*Ref, err error)

	// GetRefsFiles 用于获取所有引用索引中的文件 ID 列表 fileIDs。
	GetRefsFiles() (fileIDs []string, err error)

	// GetChunks 用于获取 checkChunkIDs 中不存在的分块 ID 列表 chunkIDs。
	GetChunks(checkChunkIDs []string) (chunkIDs []string, err error)

	// GetStat 用于获取统计信息 stat。
	GetStat() (stat *Stat, err error)

	// AddTraffic 用于统计流量上传字节数 uploadBytes 和下载字节数 downloadBytes。
	AddTraffic(uploadBytes, downloadBytes int64)
}

// Stat 描述了统计信息。
type Stat struct {
	Sync      *StatSync   `json:"sync"`      // 同步统计
	Backup    *StatBackup `json:"backup"`    // 备份统计
	AssetSize int64       `json:"assetSize"` // 资源文件大小字节数
	RepoCount int         `json:"repoCount"` // 仓库数量
}

// Repo 描述了云端仓库。
type Repo struct {
	Name    string `json:"name"`
	Size    int64  `json:"size"`
	Updated string `json:"updated"`
}

// Ref 描述了快照引用。
type Ref struct {
	Name    string `json:"name"`    // 引用文件名称，比如 latest、tag1
	ID      string `json:"id"`      // 引用 ID
	Updated string `json:"updated"` // 最近更新时间
}

// StatSync 描述了同步统计信息。
type StatSync struct {
	Size      int64  `json:"size"`      // 总大小字节数
	FileCount int    `json:"fileCount"` // 总文件数
	Updated   string `json:"updated"`   // 最近更新时间
}

// StatBackup 描述了备份统计信息。
type StatBackup struct {
	Count     int    `json:"count"`     // 已标记的快照数量
	Size      int64  `json:"size"`      // 总大小字节数
	FileCount int    `json:"fileCount"` // 总文件数
	Updated   string `json:"updated"`   // 最近更新时间
}

// BaseCloud 描述了云端存储服务的基础实现。
type BaseCloud struct {
	*Conf
	Cloud
}

func (baseCloud *BaseCloud) GetAvailableSize() (size int64) {
	return baseCloud.Conf.AvailableSize
}

func (baseCloud *BaseCloud) GetConf() *Conf {
	return baseCloud.Conf
}

func (baseCloud *BaseCloud) AddTraffic(uploadBytes, downloadBytes int64) {
	return
}

var (
	// ErrCloudObjectNotFound 描述了云端存储服务中的对象不存在的错误。
	ErrCloudObjectNotFound = errors.New("cloud object not found")
	// ErrCloudAuthFailed 描述了云端存储服务鉴权失败的错误。
	ErrCloudAuthFailed = errors.New("cloud account auth failed")
)

func IsValidCloudDirName(cloudDirName string) bool {
	if 16 < utf8.RuneCountInString(cloudDirName) || 1 > utf8.RuneCountInString(cloudDirName) {
		return false
	}

	chars := []byte{'~', '`', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '+', '=',
		'[', ']', '{', '}', '\\', '|', ';', ':', '\'', '"', '<', ',', '>', '.', '?', '/', ' '}
	var charsStr string
	for _, char := range chars {
		charsStr += string(char)
	}

	if strings.ContainsAny(cloudDirName, charsStr) {
		return false
	}

	tmp := stripCtlFromUTF8(cloudDirName)
	return tmp == cloudDirName
}

func stripCtlFromUTF8(str string) string {
	return strings.Map(func(r rune) rune {
		if r >= 32 && r != 127 {
			return r
		}
		return -1
	}, str)
}
