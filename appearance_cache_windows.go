package dejavu

import (
	"encoding/binary"
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

func appearanceFileChangeStamp(abs string, info os.FileInfo) (string, error) {
	file, err := os.Open(abs)
	if err != nil {
		return "", err
	}
	defer file.Close()
	var identity windows.ByHandleFileInformation
	var basic struct {
		CreationTime, LastAccessTime, LastWriteTime, ChangeTime int64
		FileAttributes, Reserved                                uint32
	}
	handle := windows.Handle(file.Fd())
	if windows.GetFileInformationByHandle(handle, &identity) != nil ||
		windows.GetFileInformationByHandleEx(handle, windows.FileBasicInfo, (*byte)(unsafe.Pointer(&basic)), uint32(unsafe.Sizeof(basic))) != nil {
		// 部分虚拟文件系统不提供变化时间，保留完整读取和认证。
		return "", nil
	}
	if basic.ChangeTime == 0 || identity.FileIndexHigh == 0 && identity.FileIndexLow == 0 {
		return "", nil
	}
	// 时间戳可能在同一系统时钟刻度内重复，额外要求文件的 USN 变化计数；不支持时完整重读。
	// 输入和输出均使用至少四字节对齐的存储，避免系统拒绝未对齐的用户缓冲区。
	versions := uint32(2 | 3<<16)
	var words [512]uint64
	record := unsafe.Slice((*byte)(unsafe.Pointer(&words[0])), 4096)
	var returned uint32
	if windows.DeviceIoControl(handle, windows.FSCTL_READ_FILE_USN_DATA, (*byte)(unsafe.Pointer(&versions)), 4,
		&record[0], uint32(len(record)), &returned, nil) != nil || returned < 32 {
		return "", nil
	}
	offset := 24
	switch binary.LittleEndian.Uint16(record[4:6]) {
	case 2:
	case 3:
		offset = 40
	default:
		return "", nil
	}
	if returned < uint32(offset+8) {
		return "", nil
	}
	usn := binary.LittleEndian.Uint64(record[offset : offset+8])
	if usn == 0 || usn > 1<<63-1 {
		return "", nil
	}
	return fmt.Sprintf("%d:%d:%d:%d:%d:%d:%d:%d:%d", identity.VolumeSerialNumber, identity.FileIndexHigh,
		identity.FileIndexLow, info.Size(), info.ModTime().UnixNano(), basic.CreationTime, basic.LastWriteTime, basic.ChangeTime, usn), nil
}
