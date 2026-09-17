// DejaVu - Data snapshot and sync.
// Copyright (c) 2022-present, b3log.org
// SPDX-License-Identifier: AGPL-3.0-or-later

package dejavu

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"

	"github.com/siyuan-note/dejavu/entity"
)

// ReadPendingAppearancePackages 只读认证后的待恢复外观包，不获取仓库锁或触发网络与文件恢复。
func ReadPendingAppearancePackages(statePath string, aesKey []byte) ([]string, error) {
	data, err := readAssetStateFile(statePath)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) < 28 {
		return nil, ErrAssetDownloadState
	}
	store, err := NewStore("", aesKey)
	if err != nil {
		return nil, err
	}
	defer store.compressDecoder.Close()
	defer store.compressEncoder.Close()
	data, err = store.decodeData(data)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrAssetDownloadState, err)
	}
	var state assetDownloadState
	if err = json.Unmarshal(data, &state); err != nil || state.Version != 1 || state.Scope == "" || state.Deferred == nil {
		return nil, ErrAssetDownloadState
	}
	keys := map[string]bool{}
	for p, file := range state.Deferred {
		if !validAssetFile(file) || p != file.Path || !IsAssetDownloadPath(p) {
			return nil, ErrAssetDownloadState
		}
		if key := appearancePackageKey(p); key != "" {
			keys[key] = true
		}
	}
	if state.Pending == nil {
		return pendingAppearanceKeys(keys), nil
	}
	pending := state.Pending
	if pending.Index == nil || pending.Base == nil || pending.Deferred == nil || pending.Before == nil {
		return nil, ErrAssetDownloadState
	}
	for p, file := range pending.Deferred {
		if !validAssetFile(file) || p != file.Path || !IsAssetDownloadPath(p) {
			return nil, ErrAssetDownloadState
		}
		if key := appearancePackageKey(p); key != "" {
			keys[key] = true
		}
	}
	for _, files := range [][]*entity.File{pending.Upserts, pending.Removes} {
		for _, file := range files {
			if !validAssetFile(file) || (pending.Before[file.Path] != nil && !validAssetFile(pending.Before[file.Path])) {
				return nil, ErrAssetDownloadState
			}
			if key := appearancePackageKey(file.Path); key != "" {
				keys[key] = true
			}
		}
	}
	return pendingAppearanceKeys(keys), nil
}

func pendingAppearanceKeys(keys map[string]bool) []string {
	ret := make([]string, 0, len(keys))
	for key := range keys {
		ret = append(ret, key)
	}
	sort.Strings(ret)
	return ret
}
