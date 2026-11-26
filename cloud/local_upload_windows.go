package cloud

import (
	"os"

	"golang.org/x/sys/windows"
)

func publishLocalUpload(source, target string) error {
	err := os.Link(source, target)
	if err == nil || os.IsExist(err) {
		return err
	}
	sourcePath, err := windows.UTF16PtrFromString(source)
	if err != nil {
		return err
	}
	targetPath, err := windows.UTF16PtrFromString(target)
	if err != nil {
		return err
	}
	return windows.MoveFile(sourcePath, targetPath)
}
