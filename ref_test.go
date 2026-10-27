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
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTagSerializesWithPurge(t *testing.T) {
	base := t.TempDir()
	repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "tag-purge", false)
	old, err := repo.Latest()
	if err != nil {
		t.Fatal(err)
	}
	writeAssetTestFile(t, repo, "/seed.txt", "new contents", 2)
	if _, err := repo.Index("new", true, nil); err != nil {
		t.Fatal(err)
	}
	// Hold the same lock as Repo.Purge and stop just before its mark/sweep.
	lock.Lock()
	locked := true
	defer func() {
		if locked {
			lock.Unlock()
		}
	}()
	started, done := make(chan struct{}), make(chan error, 1)
	go func() { close(started); done <- repo.AddTag(old.ID, "retained") }()
	<-started
	select {
	case err := <-done:
		t.Fatalf("tag publication bypassed purge lock: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := repo.store.Purge(context.Background()); err != nil {
		t.Fatal(err)
	}
	lock.Unlock()
	locked = false
	if err := <-done; err == nil {
		t.Fatal("tagging a purged snapshot must fail")
	}
	if _, err := os.Stat(filepath.Join(repo.Path, "refs", "tags", "retained")); !os.IsNotExist(err) {
		t.Fatalf("purged snapshot received a dangling tag: %v", err)
	}

	latest, err := repo.Latest()
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.AddTag(latest.ID, "retained"); err != nil {
		t.Fatal(err)
	}
	writeAssetTestFile(t, repo, "/seed.txt", "another version", 3)
	if _, err := repo.Index("newer", true, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Purge(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.GetIndex(latest.ID); err != nil {
		t.Fatalf("published tag did not retain snapshot: %v", err)
	}
}

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
