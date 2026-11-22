package dejavu

import (
	"os"
	"syscall"

	"github.com/88250/gulu"
)

func isDirectoryReparsePoint(info os.FileInfo) bool {
	data, ok := info.Sys().(*syscall.Win32FileAttributeData)
	const junction = syscall.FILE_ATTRIBUTE_DIRECTORY | syscall.FILE_ATTRIBUTE_REPARSE_POINT
	return ok && info.Mode()&os.ModeIrregular != 0 && data.FileAttributes&junction == junction
}

func hiddenFile(info os.FileInfo, absPath string) bool {
	if info.Sys() == nil {
		return false
	}
	if data, ok := info.Sys().(*syscall.Win32FileAttributeData); ok {
		return data.FileAttributes&syscall.FILE_ATTRIBUTE_HIDDEN != 0
	}
	return gulu.File.IsHidden(absPath)
}
