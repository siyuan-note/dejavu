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
