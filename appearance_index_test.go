package dejavu

import (
	"bytes"
	"errors"
	"os"
	"testing"
	"time"
)

// appearanceWalkInfo 模拟安卓遍历返回的文件信息，仅提供毫秒精度且不包含原生文件身份。
type appearanceWalkInfo struct {
	os.FileInfo
	onSize func()
}

func (info appearanceWalkInfo) ModTime() time.Time {
	return time.UnixMilli(info.FileInfo.ModTime().UnixMilli())
}

func (info appearanceWalkInfo) Sys() interface{} {
	return nil
}

func (info appearanceWalkInfo) Size() int64 {
	if info.onSize != nil {
		info.onSize()
	}
	return info.FileInfo.Size()
}

func TestAppearanceSnapshotAndroidFileInfo(t *testing.T) {
	for _, change := range []string{"unchanged", "size", "timestamp", "content", "replacement"} {
		t.Run(change, func(t *testing.T) {
			repo, _ := newAppearanceRefRepo(t)
			archive := appearanceRefEvent(t, false)
			if _, err := repo.writeAppearanceArchive(archive); err != nil {
				t.Fatal(err)
			}
			p := appearanceArchivePath(archive)
			abs := repo.absPath(p)
			stamp := time.Unix(1700000000, 123456700)
			if err := os.Chtimes(abs, stamp, stamp); err != nil {
				t.Fatal(err)
			}
			native, err := os.Stat(abs)
			if err != nil {
				t.Fatal(err)
			}
			info := appearanceWalkInfo{FileInfo: native}
			data, err := os.ReadFile(abs)
			if err != nil {
				t.Fatal(err)
			}
			switch change {
			case "size":
				data = append(data, 0)
			case "timestamp":
				stamp = stamp.Add(time.Second)
			case "content":
				data[len(data)-1] ^= 1
			case "replacement":
				calls := 0
				info.onSize = func() {
					calls++
					if calls != 2 {
						return
					}
					// 在原生身份采样后替换为相同内容和时间的文件，验证身份检查仍然生效。
					if err := os.Rename(abs, abs+".previous"); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(abs, data, 0644); err != nil {
						t.Fatal(err)
					}
					if err := os.Chtimes(abs, native.ModTime(), native.ModTime()); err != nil {
						t.Fatal(err)
					}
				}
			}
			if change == "size" || change == "content" {
				if err := os.WriteFile(abs, data, 0644); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Chtimes(abs, stamp, stamp); err != nil {
				t.Fatal(err)
			}
			file, err := repo.snapshotFile(abs, p, info)
			if change == "unchanged" {
				if err != nil || file == nil || file.Size != native.Size() || file.Updated != native.ModTime().UnixMilli() {
					t.Fatalf("unchanged Android file rejected: %+v %v", file, err)
				}
			} else if err == nil || file != nil {
				t.Fatalf("changed archive accepted: %+v %v", file, err)
			} else if change != "content" && !errors.Is(err, ErrIndexFileChanged) {
				t.Fatalf("expected file change error: %v", err)
			}
			preserved, readErr := os.ReadFile(abs)
			if readErr != nil || !bytes.Equal(preserved, data) {
				t.Fatalf("scan changed source bytes: %v", readErr)
			}
		})
	}
}
