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
	"os"
	"testing"
)

func TestSync(t *testing.T) {
	repo, _ := initIndex(t)

	userId := os.Getenv("dejavu_sync_user")
	token := os.Getenv("dejavu_sync_token")
	if "" == token || "" == userId {
		return
	}

	cloudInfo := &CloudInfo{
		Dir:       "test",
		UserID:    userId,
		LimitSize: 1024 * 1024 * 1024 * 8,
		Token:     token,
		Server:    "http://127.0.0.1:64388",
	}
	latest, mergeResult, trafficStat, err := repo.Sync(cloudInfo, nil)
	if nil != err {
		t.Fatalf("sync failed: %s", err)
		return
	}
	_ = latest
	_ = mergeResult
	_ = trafficStat
}
