//go:build !windows

package dejavu

import "os"

func hiddenFile(info os.FileInfo, absPath string) bool {
	return false
}
