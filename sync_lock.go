// DejaVu - Data snapshot and sync.
// Copyright (c) 2022-present, b3log.org
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package dejavu

import (
	"errors"
	"path/filepath"
	"strings"
	"time"

	"github.com/88250/gulu"
	"github.com/siyuan-note/dejavu/cloud"
	"github.com/siyuan-note/eventbus"
	"github.com/siyuan-note/logging"
)

var (
	ErrLockCloudFailed = errors.New("lock cloud repo failed")
	ErrCloudLocked     = errors.New("cloud repo is locked")
)

const (
	lockSyncKey = "lock-sync"
)

func (repo *Repo) unlockCloud(context map[string]interface{}) {
	endRefreshLock <- true
	var err error
	for i := 0; i < 3; i++ {
		eventbus.Publish(eventbus.EvtCloudUnlock, context)
		err = repo.cloud.RemoveObject(lockSyncKey)
		if nil == err {
			return
		}
	}

	if errors.Is(err, cloud.ErrCloudAuthFailed) {
		return
	}

	logging.LogErrorf("unlock cloud repo failed: %s", err)
	return
}

var endRefreshLock = make(chan bool)

func (repo *Repo) tryLockCloud(currentDeviceID string, context map[string]interface{}) (err error) {
	for i := 0; i < 3; i++ {
		err = repo.lockCloud(currentDeviceID, context)
		if nil != err {
			if errors.Is(err, ErrCloudLocked) {
				logging.LogInfof("cloud repo is locked, retry after 5s")
				time.Sleep(5 * time.Second)
				continue
			}
			return
		}

		// 锁定成功，定时刷新锁
		go func() {
			for {
				select {
				case <-endRefreshLock:
					return
				case <-time.After(30 * time.Second):
					if refershErr := repo.lockCloud0(currentDeviceID); nil != refershErr {
						logging.LogErrorf("refresh cloud repo lock failed: %s", refershErr)
					}
				}
			}
		}()

		return
	}
	return
}

// lockCloud 锁定云端仓库，不要单独调用，应该调用 tryLockCloud，否则解锁时 endRefreshLock 会阻塞。
func (repo *Repo) lockCloud(currentDeviceID string, context map[string]interface{}) (err error) {
	eventbus.Publish(eventbus.EvtCloudLock, context)
	data, err := repo.cloud.DownloadObject(lockSyncKey)
	if errors.Is(err, cloud.ErrCloudObjectNotFound) {
		err = repo.lockCloud0(currentDeviceID)
		return
	}

	content := map[string]interface{}{}
	err = gulu.JSON.UnmarshalJSON(data, &content)
	if nil != err {
		logging.LogErrorf("unmarshal lock sync failed: %s", err)
		err = repo.cloud.RemoveObject(lockSyncKey)
		if nil != err {
			logging.LogErrorf("remove unmarshalled lock sync failed: %s", err)
		} else {
			err = repo.lockCloud0(currentDeviceID)
		}

		if ok, retErr := parseErr(err); ok {
			return retErr
		}
		return
	}

	deviceID := content["deviceID"].(string)
	t := int64(content["time"].(float64))
	now := time.Now()
	lockTime := time.UnixMilli(t)
	if now.After(lockTime.Add(65*time.Second)) || deviceID == currentDeviceID {
		// 云端锁超时过期或者就是当前设备锁的，那么当前设备可以继续直接锁
		err = repo.lockCloud0(currentDeviceID)
		return
	}

	logging.LogWarnf("cloud repo is locked by device [%s] at [%s], will retry after 30s", content["deviceID"].(string), lockTime.Format("2006-01-02 15:04:05"))
	err = ErrCloudLocked
	return
}

func (repo *Repo) lockCloud0(currentDeviceID string) (err error) {
	lockSyncPath := filepath.Join(repo.Path, lockSyncKey)
	content := map[string]interface{}{
		"deviceID": currentDeviceID,
		"time":     time.Now().UnixMilli(),
	}
	data, err := gulu.JSON.MarshalJSON(content)
	if nil != err {
		logging.LogErrorf("marshal lock sync failed: %s", err)
		err = ErrLockCloudFailed
		return
	}
	err = gulu.File.WriteFileSafer(lockSyncPath, data, 0644)
	if nil != err {
		logging.LogErrorf("write lock sync failed: %s", err)
		err = ErrCloudLocked
		return
	}

	_, err = repo.cloud.UploadObject(lockSyncKey, true)
	if nil != err {
		if errors.Is(err, cloud.ErrSystemTimeIncorrect) || errors.Is(err, cloud.ErrCloudAuthFailed) || errors.Is(err, cloud.ErrDeprecatedVersion) ||
			errors.Is(err, cloud.ErrCloudCheckFailed) {
			return
		}

		logging.LogErrorf("upload lock sync failed: %s", err)
		if ok, retErr := parseErr(err); ok {
			return retErr
		}

		err = ErrLockCloudFailed
	}
	return
}

func parseErr(err error) (bool, error) {
	if nil == err {
		return true, nil
	}

	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "requesttimetooskewed") || strings.Contains(msg, "request time and the current time is too large") {
		return true, cloud.ErrSystemTimeIncorrect
	}

	if strings.Contains(msg, "unavailable") {
		return true, cloud.ErrCloudServiceUnavailable
	}
	return false, err
}
