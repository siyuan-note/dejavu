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

package cloud

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLocalUploadObject(t *testing.T) {
	tempDir := t.TempDir()
	repoPath := filepath.Join(tempDir, "repo")
	endpoint := filepath.Join(tempDir, "cloud")
	sourcePath := filepath.Join(repoPath, "objects", "test")
	if err := os.MkdirAll(filepath.Dir(sourcePath), 0755); nil != err {
		t.Fatal(err)
	}
	if err := os.WriteFile(sourcePath, []byte("first"), 0644); nil != err {
		t.Fatal(err)
	}

	local := NewLocal(&BaseCloud{Conf: &Conf{
		Dir:      "main",
		RepoPath: repoPath,
		Local:    &ConfLocal{Endpoint: endpoint},
	}})
	length, err := local.UploadObject("objects/test", false)
	if nil != err {
		t.Fatal(err)
	}
	if int64(len("first")) != length {
		t.Fatalf("unexpected upload length [%d]", length)
	}

	targetPath := filepath.Join(endpoint, "main", "objects", "test")
	data, err := os.ReadFile(targetPath)
	if nil != err {
		t.Fatal(err)
	}
	if "first" != string(data) {
		t.Fatalf("unexpected uploaded data [%s]", data)
	}

	if err = os.WriteFile(sourcePath, []byte("second"), 0644); nil != err {
		t.Fatal(err)
	}
	length, err = local.UploadObject("objects/test", false)
	if nil != err {
		t.Fatal(err)
	}
	if 0 != length {
		t.Fatalf("non-overwrite upload returned length [%d]", length)
	}
	data, err = os.ReadFile(targetPath)
	if nil != err {
		t.Fatal(err)
	}
	if "first" != string(data) {
		t.Fatalf("non-overwrite upload changed data [%s]", data)
	}

	length, err = local.UploadObject("objects/test", true)
	if nil != err {
		t.Fatal(err)
	}
	if int64(len("second")) != length {
		t.Fatalf("unexpected overwrite upload length [%d]", length)
	}
	data, err = os.ReadFile(targetPath)
	if nil != err {
		t.Fatal(err)
	}
	if "second" != string(data) {
		t.Fatalf("overwrite upload did not change data [%s]", data)
	}
}
