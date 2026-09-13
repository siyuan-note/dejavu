package dejavu

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// IgnorePath 判定文件系统不变量，规则文件的点前缀祖先目录允许遍历。
// info 为空时仅检查路径名称，供调用方判断已删除的文件。
func IgnorePath(info os.FileInfo, absPath, relPath, rulePath string, hiddenDirectoryNames ...string) (bool, error) {
	name := filepath.Base(absPath)
	if info != nil && info.IsDir() {
		if strings.HasPrefix(name, ".") {
			rel := strings.TrimPrefix(filepath.ToSlash(relPath), "/")
			if slices.Contains(hiddenDirectoryNames, name) || (rel != "" && strings.HasPrefix(rulePath, rel+"/")) {
				return false, nil
			}
			return true, filepath.SkipDir
		}
		// 保留已发布版本对目录隐藏属性的处理，避免改变索引集合。
		return false, nil
	}
	if strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".tmp") {
		return true, nil
	}
	return info != nil && (!info.Mode().IsRegular() || hiddenFile(info, absPath)), nil
}
