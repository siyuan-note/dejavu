package dejavu

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

func appearanceWindowsTestUSN(t *testing.T, p string) uint64 {
	t.Helper()
	file, err := os.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	versions := uint32(2 | 3<<16)
	var words [512]uint64
	record := unsafe.Slice((*byte)(unsafe.Pointer(&words[0])), 4096)
	var returned uint32
	if err = windows.DeviceIoControl(windows.Handle(file.Fd()), windows.FSCTL_READ_FILE_USN_DATA,
		(*byte)(unsafe.Pointer(&versions)), 4, &record[0], uint32(len(record)), &returned, nil); err != nil {
		t.Skipf("file system does not expose per-file USN: %v", err)
	}
	if returned < 48 {
		t.Skip("file system returned an unsupported USN record")
	}
	offset := 24
	if binary.LittleEndian.Uint16(record[4:6]) == 3 {
		offset = 40
	} else if binary.LittleEndian.Uint16(record[4:6]) != 2 {
		t.Skip("file system returned an unsupported USN version")
	}
	usn := binary.LittleEndian.Uint64(record[offset : offset+8])
	if usn == 0 || usn > 1<<63-1 {
		t.Skip("file system does not assign a valid USN")
	}
	return usn
}

func TestAppearanceWindowsStampUsesAlignedUSN(t *testing.T) {
	p := filepath.Join(t.TempDir(), "archive.bin")
	stamp := time.Unix(946684800, 0)
	var previous string
	var previousUSN uint64
	for i := 0; i < 8; i++ {
		if err := os.WriteFile(p, []byte{byte(i), 1, 2, 3, 4}, 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, stamp, stamp); err != nil {
			t.Fatal(err)
		}
		usn := appearanceWindowsTestUSN(t, p)
		info, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		current, err := appearanceFileChangeStamp(p, info)
		if err != nil || current == "" {
			t.Fatalf("available USN did not produce a cache identity: %v", err)
		}
		if i != 0 && (current == previous || usn <= previousUSN) {
			t.Fatal("preserved mtime concealed a completed file change")
		}
		previous, previousUSN = current, usn
	}
}
