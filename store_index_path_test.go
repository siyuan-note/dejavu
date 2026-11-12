package dejavu

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/siyuan-note/dejavu/entity"
)

func TestIndexStoreRejectsTraversalIndexID(t *testing.T) {
	root := t.TempDir()
	store, err := NewStore(filepath.Join(root, "repo"), []byte("0123456789abcdef0123456789abcdef"))
	if nil != err {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		store.compressEncoder.Close()
		store.compressDecoder.Close()
	})

	// 布局与 SiYuan 生产一致：数据目录和仓库目录是兄弟目录。
	targetPath := filepath.Join(root, "data", "notebook", "document.sy")
	original := []byte(`{"ID":"20260101000000-doc001","Type":"NodeDocument"}`)
	if err = os.MkdirAll(filepath.Dir(targetPath), 0755); nil != err {
		t.Fatal(err)
	}
	if err = os.WriteFile(targetPath, original, 0644); nil != err {
		t.Fatal(err)
	}

	traversalID := "..//../././././data/notebook/document.sy"
	if err = store.PutIndex(&entity.Index{ID: traversalID}); !errors.Is(err, errInvalidIndexID) {
		t.Fatalf("put accepted traversal index ID: %v", err)
	}
	if _, err = store.GetIndex(traversalID); !errors.Is(err, errInvalidIndexID) {
		t.Fatalf("get accepted traversal index ID: %v", err)
	}
	post, err := os.ReadFile(targetPath)
	if nil != err || !bytes.Equal(post, original) {
		t.Fatalf("note outside indexes was overwritten: %q, %v", post, err)
	}

	validID := "0123456789abcdef0123456789abcdef01234567"
	index := &entity.Index{ID: validID, Created: 1}
	index.InitAESKeyVerifyVal(store.AesKey)
	if err = store.PutIndex(index); nil != err {
		t.Fatalf("put valid index failed: %v", err)
	}
	if _, err = store.GetIndex(validID); nil != err {
		t.Fatalf("get valid index failed: %v", err)
	}
}
