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
