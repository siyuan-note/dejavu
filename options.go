package dejavu

import (
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/siyuan-note/dejavu/cloud"
)

// Options 显式指定仓库路径、设备信息和应用层忽略策略。
type Options struct {
	DataPath, RepoPath, HistoryPath, TempPath string
	DeviceID, DeviceName, DeviceOS            string
	AESKey                                    []byte
	IgnoreLines                               []string
	IgnoreRulePath                            string
	Cloud                                     cloud.Cloud
	// HiddenDirectoryNames 显式保留应用允许遍历的点前缀目录名称。
	HiddenDirectoryNames []string
	// PathFilter 返回是否忽略当前条目，目录剪枝通过 filepath.SkipDir 表示。
	PathFilter func(os.FileInfo, string) (bool, error)
}

// NewRepoWithOptions 创建使用显式应用策略的仓库，NewRepo 保留旧调用方的忽略行为。
func NewRepoWithOptions(options Options) (*Repo, error) {
	rulePath := options.IgnoreRulePath
	if rulePath != "" && (strings.ContainsAny(rulePath, "\\:\x00") || strings.HasPrefix(rulePath, "/") ||
		path.Clean(rulePath) != rulePath || rulePath == "." || rulePath == ".." || strings.HasPrefix(rulePath, "../")) {
		return nil, fmt.Errorf("invalid relative ignore rule path: %q", rulePath)
	}
	repo, err := NewRepo(options.DataPath, options.RepoPath, options.HistoryPath, options.TempPath,
		options.DeviceID, options.DeviceName, options.DeviceOS, options.AESKey, options.IgnoreLines, options.Cloud)
	if err != nil {
		return nil, err
	}
	repo.ignoreRulePath = ""
	if rulePath != "" {
		repo.ignoreRulePath = "/" + rulePath
	}
	repo.pathFilter = func(info os.FileInfo, absPath string) (bool, error) {
		if ignored, err := IgnorePath(info, absPath, repo.relPath(absPath), rulePath, options.HiddenDirectoryNames...); ignored || err != nil {
			return ignored, err
		}
		if options.PathFilter != nil {
			if ignored, err := options.PathFilter(info, absPath); ignored || err != nil {
				return ignored, err
			}
		}
		return info.IsDir(), nil
	}
	return repo, nil
}
