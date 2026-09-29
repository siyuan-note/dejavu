package dejavu

import (
	"errors"
	"reflect"
	"testing"

	"github.com/siyuan-note/dejavu/cloud"
	"github.com/siyuan-note/dejavu/entity"
)

type purgeResultCloud struct {
	cloud.Cloud
	objects    map[string]*entity.ObjectInfo
	indexes    map[string]*entity.ObjectInfo
	failPrefix string
	err        error
	removed    []string
}

func (c *purgeResultCloud) DownloadObject(string) ([]byte, error) {
	return nil, cloud.ErrCloudObjectNotFound
}

func (c *purgeResultCloud) UploadObject(string, bool) (int64, error) {
	return 0, nil
}

func (c *purgeResultCloud) RemoveObject(key string) error {
	c.removed = append(c.removed, key)
	return nil
}

func (c *purgeResultCloud) ListObjects(prefix string) (map[string]*entity.ObjectInfo, error) {
	if prefix == c.failPrefix {
		return nil, c.err
	}
	if prefix == "objects/" {
		return c.objects, nil
	}
	return c.indexes, nil
}

func TestPurgeCloudSkippedResult(t *testing.T) {
	for _, tc := range []struct {
		name             string
		objects, indexes bool
	}{
		{"empty", false, false}, {"no indexes", true, false}, {"no objects", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := &purgeResultCloud{}
			if tc.objects {
				backend.objects = map[string]*entity.ObjectInfo{"ab/cd": {Size: 4}}
			}
			if tc.indexes {
				backend.indexes = map[string]*entity.ObjectInfo{"index": {Size: 4}}
			}
			repo := &Repo{Path: t.TempDir(), cloud: backend}
			stat, err := repo.PurgeCloud()
			if err != nil || stat == nil || *stat != (entity.PurgeStat{}) {
				t.Fatalf("expected zero purge statistics, got stat=%+v err=%v", stat, err)
			}
			if !reflect.DeepEqual(backend.removed, []string{lockSyncKey}) {
				t.Fatalf("skip must only release its lock: %v", backend.removed)
			}
		})
	}
}

func TestPurgeCloudListErrorRemainsFailure(t *testing.T) {
	for _, prefix := range []string{"objects/", "indexes/"} {
		t.Run(prefix, func(t *testing.T) {
			cause := errors.New("listing failed")
			backend := &purgeResultCloud{failPrefix: prefix, err: cause}
			repo := &Repo{Path: t.TempDir(), cloud: backend}
			_, err := repo.PurgeCloud()
			if !errors.Is(err, cause) {
				t.Fatalf("listing error was lost: %v", err)
			}
			if !reflect.DeepEqual(backend.removed, []string{lockSyncKey}) {
				t.Fatalf("failed listing must only release its lock: %v", backend.removed)
			}
		})
	}
}
