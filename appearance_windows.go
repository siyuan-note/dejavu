package dejavu

import "syscall"

func copyAppearanceAttributes(source, destination string) error {
	from, err := syscall.UTF16PtrFromString(source)
	if err != nil {
		return err
	}
	attributes, err := syscall.GetFileAttributes(from)
	if err != nil {
		return err
	}
	to, err := syscall.UTF16PtrFromString(destination)
	if err != nil {
		return err
	}
	attributes &= syscall.FILE_ATTRIBUTE_READONLY | syscall.FILE_ATTRIBUTE_HIDDEN | syscall.FILE_ATTRIBUTE_SYSTEM | syscall.FILE_ATTRIBUTE_ARCHIVE
	if attributes == 0 {
		attributes = syscall.FILE_ATTRIBUTE_NORMAL
	}
	return syscall.SetFileAttributes(to, attributes)
}
