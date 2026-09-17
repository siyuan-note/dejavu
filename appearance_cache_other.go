//go:build !windows

package dejavu

import (
	"fmt"
	"os"
	"reflect"
	"time"
)

func appearanceFileChangeStamp(_ string, info os.FileInfo) (string, error) {
	stat := reflect.ValueOf(info.Sys())
	if stat.Kind() == reflect.Pointer && !stat.IsNil() {
		stat = stat.Elem()
	}
	if stat.Kind() != reflect.Struct {
		return "", nil
	}
	device, inode := stat.FieldByName("Dev"), stat.FieldByName("Ino")
	if !device.IsValid() || !device.CanInterface() || !inode.IsValid() || !inode.CanInterface() {
		return "", nil
	}
	var change interface{}
	var changed time.Time
	for _, field := range []string{"Ctim", "Ctimespec"} {
		value := stat.FieldByName(field)
		if value.IsValid() && value.CanInterface() && value.Kind() == reflect.Struct &&
			value.FieldByName("Nsec").IsValid() && value.FieldByName("Sec").IsValid() {
			change = value.Interface()
			changed = time.Unix(value.FieldByName("Sec").Int(), value.FieldByName("Nsec").Int())
			break
		}
	}
	if change == nil {
		seconds, nanos := stat.FieldByName("Ctime"), stat.FieldByName("Ctimensec")
		if !seconds.IsValid() || !seconds.CanInterface() || !nanos.IsValid() || !nanos.CanInterface() {
			return "", nil
		}
		change = fmt.Sprintf("%v:%v", seconds.Interface(), nanos.Interface())
		changed = time.Unix(seconds.Int(), nanos.Int())
	}
	if changed.Nanosecond() == 0 || !appearanceCacheChangeTimeStable(changed) {
		return "", nil
	}
	return fmt.Sprintf("%v:%v:%d:%d:%v", device.Interface(), inode.Interface(), info.Size(), info.ModTime().UnixNano(), change), nil
}
