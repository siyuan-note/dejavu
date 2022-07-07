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
)

func TestTag(t *testing.T) {
	clearTestdata(t)

	repo, index := initIndex(t)
	err := repo.AddTag(index.ID, "v1.0.0")
	if nil != err {
		t.Fatalf("add tag failed: %s", err)
		return
	}

	v100, err := repo.GetTag("v1.0.0")
	if v100 != index.ID {
		t.Fatalf("get tag failed: %s", err)
		return
	}

	err = repo.AddTag(index.ID, "v1.0.1")
	if nil != err {
		t.Fatalf("add tag failed: %s", err)
		return
	}

	v101, err := repo.GetTag("v1.0.1")
	if v101 != v100 {
		t.Fatalf("get tag failed: %s", err)
		return
	}
}
