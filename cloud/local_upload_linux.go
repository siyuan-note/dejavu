package cloud

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func publishLocalUpload(source, target string) error {
	err := unix.Renameat2(unix.AT_FDCWD, source, unix.AT_FDCWD, target, unix.RENAME_NOREPLACE)
	if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP) {
		return os.Link(source, target)
	}
	return err
}
