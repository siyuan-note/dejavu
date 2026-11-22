//go:build !windows

package dejavu

import "os"

func isDirectoryReparsePoint(os.FileInfo) bool {
	return false
}

func hiddenFile(info os.FileInfo, absPath string) bool {
	return false
}
