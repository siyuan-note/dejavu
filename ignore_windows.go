package dejavu

import (
	"os"
	"syscall"

	"github.com/88250/gulu"
)

func hiddenFile(info os.FileInfo, absPath string) bool {
	if info.Sys() == nil {
		return false
	}
	if data, ok := info.Sys().(*syscall.Win32FileAttributeData); ok {
		return data.FileAttributes&syscall.FILE_ATTRIBUTE_HIDDEN != 0
	}
	return gulu.File.IsHidden(absPath)
}
