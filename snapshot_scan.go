package dejavu

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// snapshotScanDiagnostic observes the namespace after a failed scan. It never
// changes paths or relaxes removal protection. Unknown omissions may be races.
func (repo *Repo) snapshotScanDiagnostic(path string, info os.FileInfo) (detail string, deterministic bool) {
	root := filepath.Clean(repo.DataPath)
	abs := repo.absPath(path)
	rel, err := filepath.Rel(root, abs)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "diagnostic path outside data directory", false
	}
	parts := strings.Split(rel, string(filepath.Separator))
	parent := root
	for i, part := range parts {
		current := filepath.Join(parent, part)
		observed, statErr := os.Lstat(current)
		if statErr != nil {
			return fmt.Sprintf("diagnostic lstat [%s]: %v", current, statErr), false
		}
		if i < len(parts)-1 && observed.Mode()&os.ModeSymlink != 0 {
			return fmt.Sprintf("ancestor link [%s], mode [%s]; directory links are not traversed by the scan", current, observed.Mode()), true
		}
		entries, readErr := os.ReadDir(parent)
		if readErr != nil {
			return fmt.Sprintf("diagnostic parent [%s] ReadDir: %v", parent, readErr), false
		}
		exact := false
		for _, entry := range entries {
			if entry.Name() == part {
				exact = true
				break
			}
		}
		if !exact {
			expected := observed
			if i == len(parts)-1 && info != nil {
				expected = info
			}
			for _, entry := range entries {
				if !strings.EqualFold(entry.Name(), part) {
					continue
				}
				actual := filepath.Join(parent, entry.Name())
				actualInfo, e := os.Lstat(actual)
				if e == nil && os.SameFile(expected, actualInfo) {
					return fmt.Sprintf("path spelling discrepancy: expected [%s], actual [%s], same file identity verified", current, actual), true
				}
			}
			return fmt.Sprintf("cause unknown; post-scan parent [%s] has no exact entry [%s]; %s", parent, part, snapshotScanEntrySummary(entries)), false
		}
		if i == len(parts)-1 {
			return fmt.Sprintf("cause unknown; post-scan parent [%s] contains exact entry [%s]; %s", parent, part, snapshotScanEntrySummary(entries)), false
		}
		parent = current
	}
	return "cause unknown", false
}

// Keep diagnostics bounded even in directories containing many/long names.
func snapshotScanEntrySummary(entries []os.DirEntry) string {
	const maxNames = 16
	const maxRunes = 80
	names := make([]string, 0, maxNames)
	for i, entry := range entries {
		if i == maxNames {
			break
		}
		name := []rune(entry.Name())
		if len(name) > maxRunes {
			name = append(name[:maxRunes], '…')
		}
		names = append(names, fmt.Sprintf("%q", string(name)))
	}
	return fmt.Sprintf("parent entries (%d total, %d shown): [%s]", len(entries), len(names), strings.Join(names, ", "))
}
