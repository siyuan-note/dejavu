// DejaVu - Data snapshot and sync.
// Copyright (c) 2022-present, b3log.org
//
// DejaVu is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//         http://license.coscl.org.cn/MulanPSL2
//
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
//
// See the Mulan PSL v2 for more details.

package util

import (
	"os"
	"path/filepath"
)

func RemoveEmptyDirs(dir string) (err error) {
	_, err = removeEmptyDirs(dir, true)
	return
}

func removeEmptyDirs(dir string, removeSelf bool) (bool, error) {
	// Credit to: https://github.com/InfuseAI/ArtiVC/blob/main/internal/core/utils.go
	// LICENSE Apache License 2.0 https://github.com/InfuseAI/ArtiVC/blob/main/LICENSE

	var hasEntries bool

	entires, err := os.ReadDir(dir)
	if err != nil {
		return false, err
	}
	for _, entry := range entires {
		if entry.IsDir() {
			subdir := filepath.Join(dir, entry.Name())
			removed, err := removeEmptyDirs(subdir, true)
			if err != nil {
				return false, err
			}
			if !removed {
				hasEntries = true
			}
		} else {
			hasEntries = true
		}
	}

	if !hasEntries && removeSelf {
		err := os.Remove(dir)
		if err != nil {
			return false, err
		}
		return true, nil
	}

	return false, nil
}
