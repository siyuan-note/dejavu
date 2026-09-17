// DejaVu - Data snapshot and sync.
// Copyright (c) 2022-present, b3log.org
// SPDX-License-Identifier: AGPL-3.0-or-later

package dejavu

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/siyuan-note/dejavu/entity"
)

func TestReadPendingAppearancePackagesAuthenticated(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	store, err := NewStore("", key)
	if err != nil {
		t.Fatal(err)
	}
	defer store.compressEncoder.Close()
	defer store.compressDecoder.Close()
	file := func(path string) *entity.File {
		return &entity.File{ID: strings.Repeat("1", 40), Path: path, Chunks: []string{strings.Repeat("2", 40)}}
	}
	state := assetDownloadState{Version: 1, Scope: "test", Deferred: map[string]*entity.File{}, Pending: &assetApply{
		Index: &entity.Index{}, Base: &entity.Index{}, Before: map[string]*entity.File{},
		Deferred: map[string]*entity.File{"/icons/pending/assets/image.png": file("/icons/pending/assets/image.png")},
		Upserts:  []*entity.File{file("/themes/theme/theme.css"), file("/storage/bazaar/themes/theme.json"), file("/assets/file.bin")},
		Removes:  []*entity.File{file("/icons/icon/icon.js")},
	}}
	statePath := filepath.Join(t.TempDir(), "state")
	writeState := func(value any) []byte {
		t.Helper()
		data, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		data, marshalErr = store.encodeData(data)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if marshalErr = os.WriteFile(statePath, data, 0600); marshalErr != nil {
			t.Fatal(marshalErr)
		}
		return data
	}
	original := writeState(state)
	got, err := ReadPendingAppearancePackages(statePath, key)
	if err != nil || !reflect.DeepEqual(got, []string{"/icons/icon", "/icons/pending", "/themes/theme"}) {
		t.Fatalf("pending packages: %v, %v", got, err)
	}
	if _, err = ReadPendingAppearancePackages(statePath, []byte(strings.Repeat("x", 32))); err == nil {
		t.Fatal("wrong key accepted")
	}
	data, _ := os.ReadFile(statePath)
	if !reflect.DeepEqual(data, original) {
		t.Fatal("read modified authenticated state")
	}
	state.Version = 99
	writeState(state)
	if _, err = ReadPendingAppearancePackages(statePath, key); err == nil {
		t.Fatal("unknown state version accepted")
	}
	state.Version = 1
	state.Pending.Upserts = []*entity.File{nil}
	writeState(state)
	if _, err = ReadPendingAppearancePackages(statePath, key); err == nil {
		t.Fatal("invalid pending file accepted")
	}
	state.Pending = nil
	writeState(state)
	if got, err = ReadPendingAppearancePackages(statePath, key); err != nil || len(got) != 0 {
		t.Fatalf("completed state: %v, %v", got, err)
	}
	state.Deferred["/themes/legacy/assets/font.woff2"] = file("/themes/legacy/assets/font.woff2")
	writeState(state)
	if got, err = ReadPendingAppearancePackages(statePath, key); err != nil || !reflect.DeepEqual(got, []string{"/themes/legacy"}) {
		t.Fatalf("legacy deferred package: %v, %v", got, err)
	}
	if err = os.WriteFile(statePath, []byte(`{"version":1,"scope":"plaintext","deferred":{}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = ReadPendingAppearancePackages(statePath, key); err == nil {
		t.Fatal("plaintext state accepted")
	}
	if err = os.Remove(statePath); err != nil {
		t.Fatal(err)
	}
	if got, err = ReadPendingAppearancePackages(statePath, key); err != nil || len(got) != 0 {
		t.Fatalf("missing state: %v, %v", got, err)
	}
}
