package dejavu

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/88250/gulu"
	"github.com/siyuan-note/dejavu/cloud"
	"github.com/siyuan-note/dejavu/entity"
)

type indexDownloadReply struct {
	data []byte
	err  error
}

type indexDownloadCloud struct {
	cloud.Cloud
	replies   []indexDownloadReply
	downloads int
	uploads   int
	removes   int
}

func (c *indexDownloadCloud) DownloadObject(string) ([]byte, error) {
	i := c.downloads
	c.downloads++
	if i >= len(c.replies) {
		i = len(c.replies) - 1
	}
	return c.replies[i].data, c.replies[i].err
}

func (c *indexDownloadCloud) UploadObject(string, bool) (int64, error) {
	c.uploads++
	return 0, nil
}

func (c *indexDownloadCloud) RemoveObject(string) error {
	c.removes++
	return nil
}

func newIndexDownloadRepo(t *testing.T, c *indexDownloadCloud) *Repo {
	t.Helper()
	dir := t.TempDir()
	store, err := NewStore(dir, []byte("0123456789abcdef"))
	if nil != err {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		store.compressEncoder.Close()
		store.compressDecoder.Close()
	})
	return &Repo{Path: dir, store: store, cloud: c}
}

func TestCloudIndexDownloadRetry(t *testing.T) {
	for _, bad := range []string{"zstd", "json", "id", "partial"} {
		t.Run(bad, func(t *testing.T) {
			c := &indexDownloadCloud{}
			repo := newIndexDownloadRepo(t, c)
			encode := func(s string) []byte { return repo.store.compressEncoder.EncodeAll([]byte(s), nil) }
			good := encode(`{"id":"expected","files":[]}`)
			invalid := []byte("not zstd")
			switch bad {
			case "json":
				invalid = encode("{\"id\":\"\x04\"}")
			case "id":
				invalid = encode(`{"id":"other"}`)
			case "partial":
				invalid = encode(`{"id":"expected","systemName":"stale",`)
			}
			c.replies = []indexDownloadReply{{data: invalid}, {data: good}}
			n, index, err := repo.downloadCloudIndex("expected", nil)
			if nil != err || "expected" != index.ID || "" != index.SystemName || 2 != c.downloads {
				t.Fatalf("unexpected retry result: index=%+v, downloads=%d, err=%v", index, c.downloads, err)
			}
			if n != int64(len(invalid)+len(good)) || 0 != c.uploads || 0 != c.removes {
				t.Fatalf("unexpected traffic or mutation: bytes=%d, cloud=%+v", n, c)
			}
		})
	}
}

func TestCloudIndexDownloadDoesNotRetryAuthentication(t *testing.T) {
	for _, transport := range []bool{true, false} {
		t.Run(fmt.Sprint(transport), func(t *testing.T) {
			c := &indexDownloadCloud{}
			repo := newIndexDownloadRepo(t, c)
			want := cloud.ErrCloudAuthFailed
			c.replies = []indexDownloadReply{{err: want}}
			if !transport {
				want = cloud.ErrDecryptFailed
				index := &entity.Index{ID: "expected"}
				index.InitAESKeyVerifyVal([]byte("fedcba9876543210"))
				data, err := gulu.JSON.MarshalJSON(index)
				if nil != err {
					t.Fatal(err)
				}
				c.replies = []indexDownloadReply{{data: repo.store.compressEncoder.EncodeAll(data, nil)}}
			}
			_, _, err := repo.downloadCloudIndex("expected", nil)
			if !errors.Is(err, want) || 1 != c.downloads || 0 != c.uploads || 0 != c.removes {
				t.Fatalf("unexpected authentication result: cloud=%+v, err=%v", c, err)
			}
		})
	}
}

func TestCloudIndexesRetryPreservesData(t *testing.T) {
	for _, mode := range []string{"decode", "json", "empty", "null", "null entry", "disappeared", "recovered", "missing"} {
		t.Run(mode, func(t *testing.T) {
			c := &indexDownloadCloud{}
			repo := newIndexDownloadRepo(t, c)
			encode := func(s string) []byte { return repo.store.compressEncoder.EncodeAll([]byte(s), nil) }
			invalid := []byte("invalid zstd")
			switch mode {
			case "json":
				invalid = encode("{\"indexes\":\"\x04\"}")
			case "empty":
				invalid = nil
			case "null":
				invalid = encode("null")
			case "null entry":
				invalid = encode(`{"indexes":[null]}`)
			}
			c.replies = []indexDownloadReply{{data: invalid}}
			wantAttempts, wantUploads := 3, 0
			switch mode {
			case "disappeared":
				c.replies = append(c.replies, indexDownloadReply{err: cloud.ErrCloudObjectNotFound})
				wantAttempts = 2
			case "recovered":
				c.replies = append(c.replies, indexDownloadReply{data: encode(`{"indexes":[{"id":"historical"}]}`)})
				wantAttempts, wantUploads = 2, 1
			case "missing":
				c.replies = []indexDownloadReply{{err: cloud.ErrCloudObjectNotFound}}
				wantAttempts, wantUploads = 1, 1
			}
			file := filepath.Join(repo.Path, "indexes-v2.json")
			original := []byte("preserve local original")
			if err := os.WriteFile(file, original, 0644); nil != err {
				t.Fatal(err)
			}
			_, _, err := repo.updateCloudIndexesV2(&entity.Index{ID: "latest"}, nil)
			if (nil == err) != (1 == wantUploads) || c.downloads != wantAttempts || c.uploads != wantUploads || 0 != c.removes {
				t.Fatalf("unexpected update result: cloud=%+v, err=%v", c, err)
			}
			if "disappeared" == mode && errors.Is(err, cloud.ErrCloudObjectNotFound) {
				t.Fatal("corrupt object became a missing object")
			}
			data, readErr := os.ReadFile(file)
			if nil != readErr {
				t.Fatal(readErr)
			}
			if 0 == wantUploads {
				if !bytes.Equal(data, original) {
					t.Fatal("local original was overwritten")
				}
			} else {
				data, err = repo.store.compressDecoder.DecodeAll(data, nil)
				if nil != err {
					t.Fatal(err)
				}
				var indexes cloud.Indexes
				if err = gulu.JSON.UnmarshalJSON(data, &indexes); nil != err {
					t.Fatal(err)
				}
				if 0 == len(indexes.Indexes) || "latest" != indexes.Indexes[0].ID {
					t.Fatalf("missing latest: %s", data)
				}
				if "recovered" == mode && (2 != len(indexes.Indexes) || "historical" != indexes.Indexes[1].ID) {
					t.Fatalf("lost history: %s", data)
				}
			}
		})
	}
}

func TestCloudLockReadPreservesLock(t *testing.T) {
	downloadErr := errors.New("download failed")
	for _, tc := range []struct {
		name string
		data string
		err  error
		want error
	}{
		{name: "download", data: "<error>", err: downloadErr, want: downloadErr},
		{name: "download with valid body", data: `{"deviceID":"self","time":1}`, err: downloadErr, want: downloadErr},
		{name: "xml", data: "<error>"},
		{name: "null", data: "null"},
		{name: "missing fields", data: "{}"},
		{name: "wrong type", data: `{"deviceID":123,"time":"bad"}`},
		{name: "missing time", data: `{"deviceID":"self"}`},
		{name: "held", data: fmt.Sprintf(`{"deviceID":"other","time":%d}`, time.Now().UnixMilli()), want: ErrCloudLocked},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &indexDownloadCloud{replies: []indexDownloadReply{{data: []byte(tc.data), err: tc.err}}}
			repo := newIndexDownloadRepo(t, c)
			err := repo.lockCloud("self", nil)
			if nil == err || (nil != tc.want && !errors.Is(err, tc.want)) || 0 != c.uploads || 0 != c.removes {
				t.Fatalf("unexpected lock result: cloud=%+v, err=%v", c, err)
			}
			entries, err := os.ReadDir(repo.Path)
			if nil != err || 0 != len(entries) {
				t.Fatalf("unexpected local lock write: entries=%v, err=%v", entries, err)
			}
		})
	}
}

func TestCloudLockValidAcquisition(t *testing.T) {
	for _, tc := range []indexDownloadReply{
		{err: cloud.ErrCloudObjectNotFound},
		{data: []byte(`{"deviceID":"other","time":1}`)},
		{data: []byte(fmt.Sprintf(`{"deviceID":"self","time":%d}`, time.Now().UnixMilli()))},
	} {
		c := &indexDownloadCloud{replies: []indexDownloadReply{tc}}
		repo := newIndexDownloadRepo(t, c)
		if err := repo.lockCloud("self", nil); nil != err || 1 != c.uploads || 0 != c.removes {
			t.Fatalf("unexpected acquisition result: cloud=%+v, err=%v", c, err)
		}
	}
}
