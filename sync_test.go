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
	"testing"

	"github.com/siyuan-note/dejavu/transport"
)

func TestSync(t *testing.T) {
	repo, _ := initIndex(t)

	userId := "0"
	token := ""

	return

	repo.transport = &transport.SiYuan{Conf: &transport.Conf{
		Dir:       "test",
		UserID:    userId,
		LimitSize: 1024 * 1024 * 1024 * 8,
		Token:     token,
		Server:    "http://127.0.0.1:64388",
	}}

	mergeResult, trafficStat, err := repo.Sync(nil)
	if nil != err {
		t.Fatalf("sync failed: %s", err)
		return
	}
	_ = mergeResult
	_ = trafficStat
}
