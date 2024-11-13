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

	"github.com/dgraph-io/ristretto"
	"github.com/klauspost/compress/zstd"
	"github.com/siyuan-note/dejavu/entity"
)

// Conf 用于描述云端存储服务配置信息。
type Conf struct {
	Dir      string                 // 存储目录，第三方存储不使用 Dir 区别多租户
	UserID   string                 // 用户 ID，没有的话请传入一个定值比如 "0"
	RepoPath string                 // 本地仓库的绝对路径，如：F:\\SiYuan\\repo\\
	Endpoint string                 // 服务端点
	Extras   map[string]interface{} // 一些可能需要的附加信息

	// S3 对象存储协议所需配置
	S3 *ConfS3

	// WebDAV 协议所需配置
	WebDAV *ConfWebDAV

	// 以下值非官方存储服务不必传入
	Token         string // 云端接口鉴权令牌
	AvailableSize int64  // 云端存储可用空间字节数
	Server        string // 云端接口端点
}

// ConfS3 用于描述 S3 对象存储协议所需配置。
type ConfS3 struct {
	Endpoint       string // 服务端点
	AccessKey      string // Access Key
	SecretKey      string // Secret Key
	Region         string // 存储区域
	Bucket         string // 存储空间
	PathStyle      bool   // 是否使用路径风格寻址
	SkipTlsVerify  bool   //  是否跳过 TLS 验证
	Timeout        int    // 超时时间，单位：秒
	ConcurrentReqs int    // 并发请求数
}

// ConfWebDAV 用于描述 WebDAV 协议所需配置。
type ConfWebDAV struct {
	Endpoint       string // 服务端点
	Username       string // 用户名
	Password       string // 密码
	SkipTlsVerify  bool   // 是否跳过 TLS 验证
	Timeout        int    // 超时时间，单位：秒
	ConcurrentReqs int    // 并发请求数
}

// Cloud 描述了云端存储服务，接入云端存储服务时需要实现该接口。
type Cloud interface {

	// CreateRepo 用于创建名称为 name 的云端仓库。
	CreateRepo(name string) (err error)

	// RemoveRepo 用于删除云端仓库。
	RemoveRepo(name string) (err error)

	// GetRepos 用于获取云端仓库列表 repos，size 为仓库总大小字节数。
	GetRepos() (repos []*Repo, size int64, err error)

	// UploadObject 用于上传对象，overwrite 参数用于指示是否覆盖已有对象。
	UploadObject(filePath string, overwrite bool) (length int64, err error)

	// UploadBytes 用于上传对象数据 data，overwrite 参数用于指示是否覆盖已有对象。
	UploadBytes(filePath string, data []byte, overwrite bool) (length int64, err error)

	// DownloadObject 用于下载对象数据 data。
	DownloadObject(filePath string) (data []byte, err error)

	// RemoveObject 用于删除对象。
	RemoveObject(filePath string) (err error)

	// GetTags 用于获取快照标记列表。
	GetTags() (tags []*Ref, err error)

	// GetIndexes 用于获取索引列表。
	GetIndexes(page int) (indexes []*entity.Index, pageCount, totalCount int, err error)

	// GetRefsFiles 用于获取所有引用索引中的文件 ID 列表 fileIDs。
	GetRefsFiles() (fileIDs []string, refs []*Ref, err error)

	// GetChunks 用于获取 checkChunkIDs 中不存在的分块 ID 列表 chunkIDs。
	GetChunks(checkChunkIDs []string) (chunkIDs []string, err error)

	// GetStat 用于获取统计信息 stat。
	GetStat() (stat *Stat, err error)

	// GetConf 用于获取配置信息。
	GetConf() *Conf

	// GetAvailableSize 用于获取云端存储可用空间字节数。
	GetAvailableSize() (size int64)

	// AddTraffic 用于统计流量。
	AddTraffic(traffic *Traffic)

	// ListObjects 用于列出指定前缀的对象。
	ListObjects(pathPrefix string) (objInfos map[string]*entity.ObjectInfo, err error)

	// GetIndex 用于获取索引。
	GetIndex(id string) (index *entity.Index, err error)

	// GetConcurrentReqs 用于获取配置的并发请求数。
	GetConcurrentReqs() int
}

// Traffic 描述了流量信息。
type Traffic struct {
	UploadBytes   int64 // 上传字节数
	DownloadBytes int64 // 下载字节数
	APIGet        int   // API GET 请求次数
	APIPut        int   // API PUT 请求次数
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

// Indexes 描述了云端索引列表。
type Indexes struct {
	Indexes []*Index `json:"indexes"`
}

// Index 描述了云端索引。
type Index struct {
	ID         string `json:"id"`
	SystemID   string `json:"systemID"`
	SystemName string `json:"systemName"`
	SystemOS   string `json:"systemOS"`
}

// BaseCloud 描述了云端存储服务的基础实现。
type BaseCloud struct {
	*Conf
	Cloud
}

func (baseCloud *BaseCloud) CreateRepo(name string) (err error) {
	err = ErrUnsupported
	return
}

func (baseCloud *BaseCloud) RemoveRepo(name string) (err error) {
	err = ErrUnsupported
	return
}

func (baseCloud *BaseCloud) GetRepos() (repos []*Repo, size int64, err error) {
	err = ErrUnsupported
	return
}

func (baseCloud *BaseCloud) UploadObject(filePath string, overwrite bool) (err error) {
	err = ErrUnsupported
	return
}

func (baseCloud *BaseCloud) UploadBytes(filePath string, data []byte, overwrite bool) (err error) {
	err = ErrUnsupported
	return
}

func (baseCloud *BaseCloud) DownloadObject(filePath string) (data []byte, err error) {
	err = ErrUnsupported
	return
}

func (baseCloud *BaseCloud) RemoveObject(key string) (err error) {
	err = ErrUnsupported
	return
}

func (baseCloud *BaseCloud) GetTags() (tags []*Ref, err error) {
	err = ErrUnsupported
	return
}

func (baseCloud *BaseCloud) GetIndexes(page int) (indexes []*entity.Index, pageCount, totalCount int, err error) {
	err = ErrUnsupported
	return
}

func (baseCloud *BaseCloud) GetRefsFiles() (fileIDs []string, err error) {
	err = ErrUnsupported
	return
}

func (baseCloud *BaseCloud) GetChunks(checkChunkIDs []string) (chunkIDs []string, err error) {
	err = ErrUnsupported
	return
}

func (baseCloud *BaseCloud) GetStat() (stat *Stat, err error) {
	stat = &Stat{
		Sync:   &StatSync{},
		Backup: &StatBackup{},
	}
	return
}

func (baseCloud *BaseCloud) ListObjects(pathPrefix string) (objInfos map[string]*entity.ObjectInfo, err error) {
	err = ErrUnsupported
	return
}

func (baseCloud *BaseCloud) GetIndex(id string) (index *entity.Index, err error) {
	err = ErrUnsupported
	return
}

func (baseCloud *BaseCloud) GetConcurrentReqs() int {
	return 8
}

func (baseCloud *BaseCloud) GetConf() *Conf {
	return baseCloud.Conf
}

func (baseCloud *BaseCloud) GetAvailableSize() int64 {
	return baseCloud.Conf.AvailableSize
}

func (baseCloud *BaseCloud) AddTraffic(*Traffic) {
	return
}

var (
	ErrUnsupported             = errors.New("not supported yet")         // ErrUnsupported 描述了尚未支持的操作
	ErrCloudObjectNotFound     = errors.New("cloud object not found")    // ErrCloudObjectNotFound 描述了云端存储服务中的对象不存在的错误
	ErrCloudAuthFailed         = errors.New("cloud account auth failed") // ErrCloudAuthFailed 描述了云端存储服务鉴权失败的错误
	ErrCloudServiceUnavailable = errors.New("cloud service unavailable") // ErrCloudServiceUnavailable 描述了云端存储服务不可用的错误
	ErrSystemTimeIncorrect     = errors.New("system time incorrect")     // ErrSystemTimeIncorrect 描述了系统时间不正确的错误
	ErrDeprecatedVersion       = errors.New("deprecated version")        // ErrDeprecatedVersion 描述了版本过低的错误
	ErrCloudCheckFailed        = errors.New("cloud check failed")        // ErrCloudCheckFailed 描述了云端存储服务检查失败的错误
	ErrCloudForbidden          = errors.New("cloud forbidden")           // ErrCloudForbidden 描述了云端存储服务禁止访问的错误
	ErrCloudTooManyRequests    = errors.New("cloud too many requests")   // ErrCloudTooManyRequests 描述了云端存储服务请求过多的错误
)

func IsValidCloudDirName(cloudDirName string) bool {
	if 63 < len(cloudDirName) || 1 > len(cloudDirName) {
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

var (
	compressDecoder *zstd.Decoder
	cache           *ristretto.Cache[string, any]
)

func init() {
	var err error
	compressDecoder, err = zstd.NewReader(nil, zstd.WithDecoderMaxMemory(16*1024*1024*1024))
	if nil != err {
		panic(err)
	}

	cache, err = ristretto.NewCache[string, any](&ristretto.Config[string, any]{
		NumCounters: 200000,
		MaxCost:     1000 * 1000 * 32,
		BufferItems: 64,
	})
	if nil != err {
		panic(err)
	}
}

// objectInfo 描述了对象信息，用于内部处理。
type objectInfo struct {
	Key     string `json:"key"`
	Size    int64  `json:"size"`
	Updated string `json:"updated"`
}
