package dejavu

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/88250/gulu"
	"github.com/siyuan-note/dejavu/cloud"
	"github.com/siyuan-note/dejavu/entity"
)

func TestLatestRejectsInvalidRef(t *testing.T) {
	for _, test := range []struct {
		name string
		id   string
	}{
		{name: "empty"},
		{name: "truncated", id: "abc"},
		{name: "short", id: strings.Repeat("a", 39)},
		{name: "long", id: strings.Repeat("a", 41)},
		{name: "non-hex", id: strings.Repeat("g", 40)},
		{name: "whitespace", id: strings.Repeat("a", 39) + "\n"},
		{name: "path", id: "../" + strings.Repeat("a", 37)},
		{name: "windows-path", id: "..\\" + strings.Repeat("a", 37)},
	} {
		t.Run(test.name, func(t *testing.T) {
			indexCache.Clear()
			c := &indexDownloadCloud{replies: []indexDownloadReply{{err: cloud.ErrCloudAuthFailed}}}
			repo := newIndexDownloadRepo(t, c)
			refs := filepath.Join(repo.Path, "refs")
			if err := os.MkdirAll(refs, 0755); err != nil {
				t.Fatal(err)
			}
			ref := filepath.Join(refs, "latest")
			if err := os.WriteFile(ref, []byte(test.id), 0644); err != nil {
				t.Fatal(err)
			}
			got, err := repo.Latest()
			if got != nil || !errors.Is(err, ErrRepoFatal) || errors.Is(err, ErrNotFoundIndex) {
				t.Fatalf("expected invalid reference error, got index=%v, err=%v", got, err)
			}
			if c.downloads != 0 || c.uploads != 0 || c.removes != 0 {
				t.Fatalf("invalid reference accessed cloud: %+v", c)
			}
			data, err := os.ReadFile(ref)
			if err != nil || string(data) != test.id {
				t.Fatalf("invalid reference changed: data=%q, err=%v", data, err)
			}
		})
	}
}

func TestLatestRecovery(t *testing.T) {
	for _, mode := range []string{"recover", "offline", "missing", "auth", "corrupt"} {
		t.Run(mode, func(t *testing.T) {
			indexCache.Clear()
			c := &indexDownloadCloud{}
			repo := newIndexDownloadRepo(t, c)
			index := &entity.Index{ID: "0123456789abcdef0123456789abcdef01234567"}
			index.InitAESKeyVerifyVal(repo.store.AesKey)
			data, err := gulu.JSON.MarshalJSON(index)
			if err != nil {
				t.Fatal(err)
			}
			c.replies = []indexDownloadReply{{data: repo.store.compressEncoder.EncodeAll(data, nil)}}
			refs := filepath.Join(repo.Path, "refs")
			if err = os.MkdirAll(refs, 0755); err != nil {
				t.Fatal(err)
			}
			ref := filepath.Join(refs, "latest")
			if err = os.WriteFile(ref, []byte(index.ID), 0644); err != nil {
				t.Fatal(err)
			}
			_, indexPath := repo.store.IndexAbsPath(index.ID)
			switch mode {
			case "offline":
				repo.cloud = nil
			case "missing":
				c.replies = []indexDownloadReply{{err: cloud.ErrCloudObjectNotFound}}
			case "auth":
				c.replies = []indexDownloadReply{{err: cloud.ErrDecryptFailed}}
			case "corrupt":
				if err = os.MkdirAll(filepath.Dir(indexPath), 0755); err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(indexPath, []byte("corrupt"), 0644); err != nil {
					t.Fatal(err)
				}
			}
			got, err := repo.Latest()
			if mode == "recover" {
				if err != nil || got.ID != index.ID {
					t.Fatalf("recovery: %v, %v", got, err)
				}
				indexCache.Clear()
				repo.cloud = nil
				if _, err = repo.Latest(); err != nil {
					t.Fatalf("persisted recovery: %v", err)
				}
			} else {
				if err == nil || errors.Is(err, ErrNotFoundIndex) {
					t.Fatalf("unexpected error: %v", err)
				}
				if (mode == "offline" || mode == "missing") && !errors.Is(err, ErrRepoFatal) {
					t.Fatal(err)
				}
				if mode == "auth" && !errors.Is(err, cloud.ErrDecryptFailed) {
					t.Fatal(err)
				}
				if mode == "corrupt" && c.downloads != 0 {
					t.Fatal("downloaded over corrupt index")
				}
			}
			refData, err := os.ReadFile(ref)
			if err != nil || string(refData) != index.ID {
				t.Fatal("reference changed")
			}
			if c.uploads != 0 || c.removes != 0 {
				t.Fatal("cloud mutated")
			}
		})
	}
}

func TestUpdateLatestPersistsIndexBeforeRef(t *testing.T) {
	indexCache.Clear()
	repo := newIndexDownloadRepo(t, nil)
	index := &entity.Index{ID: "fedcba9876543210fedcba9876543210fedcba98"}
	if err := repo.UpdateLatest(index); err != nil {
		t.Fatal(err)
	}
	indexCache.Clear()
	if _, err := repo.Latest(); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(repo.Path, "indexes")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo.Path, "indexes"), []byte("blocked"), 0644); err != nil {
		t.Fatal(err)
	}
	next := &entity.Index{ID: "abcdef0123456789abcdef0123456789abcdef01"}
	if err := repo.UpdateLatest(next); err == nil {
		t.Fatal("expected persistence failure")
	}
	data, err := os.ReadFile(filepath.Join(repo.Path, "refs", "latest"))
	if err != nil || string(data) != index.ID {
		t.Fatal("published reference after persistence failure")
	}
}
