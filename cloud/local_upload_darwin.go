package cloud

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func publishLocalUpload(source, target string) error {
	err := unix.RenamexNp(source, target, unix.RENAME_EXCL)
	if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOTSUP) {
		return os.Link(source, target)
	}
	return err
}
