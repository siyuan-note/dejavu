package dejavu

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/88250/gulu"
	ignore "github.com/sabhiram/go-gitignore"
	"github.com/siyuan-note/dejavu/entity"
	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

const appearanceFormatPath = "/storage/appearance-v1/format.json"
const appearanceFormatData = "{\"version\":1,\"type\":\"siyuan-appearance-events\"}\n"
const appearanceEventModified = int64(946684800000)

type appearanceEvent struct {
	*appearanceArchive
	File *entity.File
}

type appearanceEventSet map[string]map[string]*appearanceEvent

func appearanceArchivePath(event *appearanceArchive) string {
	return "/storage/appearance-v1" + event.Key + "/" + event.Digest + ".sypkg"
}

func (repo *Repo) appearanceUserMatcher() *ignore.GitIgnore {
	return ignore.CompileIgnoreLines(repo.appearanceIgnoreLines...)
}

func (repo *Repo) appearanceIgnored(key string) bool {
	if info, err := os.Lstat(repo.absPath(key)); err == nil && info.Mode()&os.ModeSymlink != 0 {
		if _, stateErr := os.Lstat(repo.absPath(appearanceRecordPath(key))); errors.Is(stateErr, os.ErrNotExist) {
			return true
		}
	}
	matcher := repo.appearanceUserMatcher()
	return repo.appearanceProtocolIgnored() || appearanceWholeIgnored(matcher, key) ||
		appearanceWholeIgnored(matcher, "/storage/appearance-v1"+key)
}

func appearanceEventContainerKey(p string) string {
	const prefix = "/storage/appearance-v1/"
	if !strings.HasPrefix(p, prefix) {
		return ""
	}
	parts := strings.Split(strings.TrimPrefix(p, prefix), "/")
	if len(parts) < 2 || (parts[0] != "themes" && parts[0] != "icons") || !validAppearanceName(parts[1]) {
		return ""
	}
	return "/" + parts[0] + "/" + parts[1]
}

func (repo *Repo) appearanceProtocolIgnored() bool {
	matcher := repo.appearanceUserMatcher()
	return appearanceWholeIgnored(matcher, "/storage") || appearanceWholeIgnored(matcher, "/storage/appearance-v1")
}

func (repo *Repo) isAppearanceIndex(index *entity.Index, context map[string]interface{}) (bool, error) {
	if index == nil || repo.appearanceProtocolIgnored() {
		return false, nil
	}
	files, err := repo.getFiles(index.Files)
	if err != nil {
		return false, err
	}
	if _, err = repo.validateAppearanceFormat(index, files, false, context); err != nil {
		return false, err
	}
	marker := entity.NewFile(appearanceFormatPath, int64(len(appearanceFormatData)), appearanceEventModified)
	for _, id := range index.Files {
		if id == marker.ID {
			return true, nil
		}
	}
	return false, nil
}

func (repo *Repo) readAppearanceEvents(files []*entity.File, context map[string]interface{}) (appearanceEventSet, error) {
	ret := appearanceEventSet{}
	for _, file := range files {
		if file.Path == appearanceFormatPath {
			if repo.appearanceProtocolIgnored() {
				continue
			}
			if err := repo.ensureFileChunks(file, context); err != nil {
				return nil, err
			}
			data, err := repo.openFile(file)
			if err != nil || string(data) != appearanceFormatData {
				return nil, fmt.Errorf("invalid appearance event format marker: %v", err)
			}
			continue
		}
		key, digest := appearanceArchiveKey(file.Path)
		if key == "" {
			if strings.HasPrefix(file.Path, "/storage/appearance-v1/") && !repo.appearanceProtocolIgnored() {
				if container := appearanceEventContainerKey(file.Path); container == "" || !repo.appearanceIgnored(container) {
					return nil, fmt.Errorf("unexpected appearance event file: %s", file.Path)
				}
			}
			continue
		}
		if repo.appearanceIgnored(key) {
			continue
		}
		archive, err := repo.readAppearanceStoredArchive(file, context, false)
		if err != nil {
			return nil, err
		}
		if ret[key] == nil {
			ret[key] = map[string]*appearanceEvent{}
		}
		ret[key][digest] = &appearanceEvent{appearanceArchive: archive, File: file}
	}
	if err := hydrateAppearanceHeads(ret, func(event *appearanceEvent) (*appearanceArchive, error) {
		return repo.readAppearanceStoredArchive(event.File, context, true)
	}); err != nil {
		return nil, err
	}
	return ret, nil
}

func unionAppearanceEvents(sets ...appearanceEventSet) (appearanceEventSet, error) {
	ret := appearanceEventSet{}
	names := map[string]string{}
	for _, set := range sets {
		for key, events := range set {
			folded := cases.Fold().String(norm.NFC.String(key))
			if previous := names[folded]; previous != "" && previous != key {
				return nil, fmt.Errorf("appearance package name collision: %s and %s", previous, key)
			}
			names[folded] = key
			if ret[key] == nil {
				ret[key] = map[string]*appearanceEvent{}
			}
			for digest, event := range events {
				ret[key][digest] = event
			}
		}
	}
	for key, events := range ret {
		visiting, visited := map[string]bool{}, map[string]bool{}
		var visit func(string) error
		visit = func(id string) error {
			if visited[id] {
				return nil
			}
			if visiting[id] {
				return fmt.Errorf("cyclic appearance events: %s", key)
			}
			event := events[id]
			if event == nil {
				return fmt.Errorf("missing appearance parent event: %s/%s", key, id)
			}
			visiting[id] = true
			for _, parent := range event.Parents {
				if err := visit(parent); err != nil {
					return err
				}
			}
			delete(visiting, id)
			visited[id] = true
			return nil
		}
		for id := range events {
			if err := visit(id); err != nil {
				return nil, err
			}
		}
	}
	return ret, nil
}

func appearanceEventHeads(events map[string]*appearanceEvent) []*appearanceEvent {
	parents := map[string]bool{}
	for _, event := range events {
		for _, parent := range event.Parents {
			parents[parent] = true
		}
	}
	var ret []*appearanceEvent
	for id, event := range events {
		if !parents[id] {
			ret = append(ret, event)
		}
	}
	sort.Slice(ret, func(i, j int) bool { return ret[i].Digest < ret[j].Digest })
	return ret
}

func appearanceEventRecord(event *appearanceEvent) *appearanceRecord {
	var record appearanceRecord
	_ = json.Unmarshal(event.State, &record)
	return &record
}

// selectAppearanceEvent 先保留因果上更新的头，再对并发头执行删除优先与云端优先。
func selectAppearanceEvent(events, cloudEvents map[string]*appearanceEvent) *appearanceEvent {
	heads := appearanceEventHeads(events)
	if len(heads) == 0 {
		return nil
	}
	var deleted, remote []*appearanceEvent
	for _, head := range heads {
		if appearanceEventRecord(head).Deleted {
			deleted = append(deleted, head)
		}
	}
	candidates := heads
	if len(deleted) != 0 {
		candidates = deleted
	}
	for _, event := range candidates {
		if cloudEvents[event.Digest] != nil {
			remote = append(remote, event)
		}
	}
	if len(remote) != 0 {
		candidates = remote
	}
	return candidates[0]
}

func (repo *Repo) readAppearanceDiskEvents() (appearanceEventSet, error) {
	ret := appearanceEventSet{}
	root := repo.absPath("/storage/appearance-v1")
	err := filepath.WalkDir(root, func(abs string, entry fs.DirEntry, walkErr error) error {
		if errors.Is(walkErr, os.ErrNotExist) && abs == root {
			return nil
		}
		if walkErr != nil {
			return walkErr
		}
		if key := appearanceEventContainerKey(repo.relPath(abs)); key != "" && repo.appearanceIgnored(key) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("appearance event symlink is not supported: %s", abs)
		}
		if entry.IsDir() {
			return nil
		}
		p := repo.relPath(abs)
		if p == appearanceFormatPath {
			data, err := os.ReadFile(abs)
			if err != nil {
				return err
			}
			if string(data) != appearanceFormatData {
				return fmt.Errorf("invalid appearance event format marker")
			}
			return nil
		}
		key, digest := appearanceArchiveKey(p)
		if key == "" {
			return fmt.Errorf("unexpected appearance event file: %s", p)
		}
		if repo.appearanceIgnored(key) {
			return nil
		}
		archive, err := repo.readAppearanceDiskArchive(key, digest, abs, false)
		if err != nil {
			return err
		}
		if ret[key] == nil {
			ret[key] = map[string]*appearanceEvent{}
		}
		ret[key][digest] = &appearanceEvent{appearanceArchive: archive}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if ret, err = unionAppearanceEvents(ret); err != nil {
		return nil, err
	}
	if err = hydrateAppearanceHeads(ret, func(event *appearanceEvent) (*appearanceArchive, error) {
		return repo.readAppearanceDiskArchive(event.Key, event.Digest, repo.absPath(appearanceArchivePath(event.appearanceArchive)), true)
	}); err != nil {
		return nil, err
	}
	return ret, nil
}

func (repo *Repo) readAppearanceProjection(key string) (*appearanceArchive, error) {
	state, err := os.ReadFile(repo.absPath(appearanceRecordPath(key)))
	if err != nil {
		return nil, err
	}
	var record appearanceRecord
	if err = json.Unmarshal(state, &record); err != nil || record.Version != 1 || record.Files == nil {
		return nil, fmt.Errorf("invalid appearance package record: %s", key)
	}
	ret := &appearanceArchive{Key: key, State: state, Files: map[string][]byte{}}
	root := repo.absPath(key)
	err = filepath.WalkDir(root, func(abs string, entry fs.DirEntry, walkErr error) error {
		if errors.Is(walkErr, os.ErrNotExist) && abs == root && record.Deleted {
			return nil
		}
		if walkErr != nil {
			return walkErr
		}
		rel, relErr := filepath.Rel(root, abs)
		if relErr != nil {
			return relErr
		}
		rel = filepath.ToSlash(rel)
		if rel == "." {
			if entry.Type()&os.ModeSymlink != 0 {
				return fmt.Errorf("appearance package symlink is not supported: %s", key)
			}
			return nil
		}
		info, infoErr := entry.Info()
		if infoErr != nil {
			return infoErr
		}
		if ignoredAppearanceRelative(rel, entry.IsDir()) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if ignored, ignoredErr := IgnorePath(info, abs, "/"+rel, "", ".siyuan"); ignored || ignoredErr != nil {
			return ignoredErr
		}
		if entry.IsDir() {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("appearance package symlink is not supported: %s/%s", key, rel)
		}
		if repo.appearanceUserMatcher().MatchesPath(key + "/" + rel) {
			return fmt.Errorf("appearance package is partially ignored: %s/%s", key, rel)
		}
		data, readErr := os.ReadFile(abs)
		if readErr != nil {
			return readErr
		}
		ret.Files[rel] = data
		return nil
	})
	if err != nil {
		return nil, err
	}
	// 编码器同时校验记录中的文件集合及摘要，确保外部编辑不会被签成混合包。
	if _, err = encodeAppearanceArchive(ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func sameAppearanceProjection(left, right *appearanceArchive) bool {
	if left == nil || right == nil {
		return left == right
	}
	if !bytes.Equal(left.State, right.State) || len(left.Files) != len(right.Files) {
		return false
	}
	for p, data := range left.Files {
		if !bytes.Equal(data, right.Files[p]) {
			return false
		}
	}
	return true
}

func (repo *Repo) writeAppearanceArchive(archive *appearanceArchive) (*appearanceEvent, error) {
	data, err := encodeAppearanceArchive(archive)
	if err != nil {
		return nil, err
	}
	p := appearanceArchivePath(archive)
	abs := repo.absPath(p)
	if err = repo.writeAppearanceImmutable(abs, data); err != nil {
		return nil, err
	}
	return &appearanceEvent{appearanceArchive: archive}, nil
}

func (repo *Repo) writeAppearanceImmutable(abs string, data []byte) error {
	if err := repo.checkAppearanceParents(abs); err != nil {
		return err
	}
	if previous, err := os.ReadFile(abs); err == nil {
		if !bytes.Equal(previous, data) {
			return fmt.Errorf("immutable appearance event changed: %s", abs)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	} else {
		if err = os.MkdirAll(filepath.Dir(abs), 0755); err != nil {
			return err
		}
		if err = gulu.File.WriteFileSafer(abs, data, 0644); err != nil {
			return err
		}
	}
	stamp := time.UnixMilli(appearanceEventModified)
	return os.Chtimes(abs, stamp, stamp)
}

func (repo *Repo) prepareAppearanceEvents() error {
	if !repo.appearanceSyncEnabled || repo.appearanceProtocolIgnored() {
		return nil
	}
	if repo.appearanceUserMatcher().MatchesPath(appearanceFormatPath) {
		return fmt.Errorf("appearance event format marker is individually ignored")
	}
	if err := repo.restoreLocalAppearanceEvents(); err != nil {
		return err
	}
	events, err := repo.readAppearanceDiskEvents()
	if err != nil {
		return err
	}
	for key, packageEvents := range events {
		if repo.appearanceIgnored(key) {
			continue
		}
		for _, event := range packageEvents {
			if repo.appearanceUserMatcher().MatchesPath(appearanceArchivePath(event.appearanceArchive)) {
				return fmt.Errorf("appearance event is individually ignored: %s", event.Digest)
			}
		}
	}
	for _, kind := range []string{"themes", "icons"} {
		entries, readErr := os.ReadDir(repo.absPath("/storage/bazaar/" + kind))
		if errors.Is(readErr, os.ErrNotExist) {
			continue
		}
		if readErr != nil {
			return readErr
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
				continue
			}
			name := strings.TrimSuffix(entry.Name(), ".json")
			if !validAppearanceName(name) {
				return fmt.Errorf("invalid appearance package name: %s", name)
			}
			key := "/" + kind + "/" + name
			if repo.appearanceIgnored(key) {
				continue
			}
			if repo.appearanceUserMatcher().MatchesPath(appearanceRecordPath(key)) {
				return fmt.Errorf("appearance package record is ignored: %s", key)
			}
			projection, projectionErr := repo.readAppearanceProjection(key)
			if projectionErr != nil {
				return projectionErr
			}
			matchesHead := false
			for _, head := range appearanceEventHeads(events[key]) {
				if sameAppearanceProjection(projection, head.appearanceArchive) {
					matchesHead = true
					break
				}
			}
			if matchesHead {
				continue
			}
			var state appearanceRecord
			_ = json.Unmarshal(projection.State, &state)
			if !state.Migration {
				for _, head := range appearanceEventHeads(events[key]) {
					projection.Parents = append(projection.Parents, head.Digest)
				}
			}
			if _, err = repo.writeAppearanceArchive(projection); err != nil {
				return err
			}
		}
	}
	return repo.writeAppearanceImmutable(repo.absPath(appearanceFormatPath), []byte(appearanceFormatData))
}

func (repo *Repo) restoreLocalAppearanceEvents() error {
	stat := newAppearanceTraffic()
	root, err := repo.loadAppearanceTagIndex(false, nil, stat)
	if err != nil || root == nil {
		return err
	}
	if err = repo.validateAppearanceRecoveryIndex(root, false, nil, stat); err != nil {
		return err
	}
	files, err := repo.getFiles(root.Files)
	if err != nil {
		return err
	}
	for _, file := range files {
		key, _ := appearanceArchiveKey(file.Path)
		if key == "" || repo.appearanceIgnored(key) {
			continue
		}
		if info, statErr := os.Lstat(repo.absPath(file.Path)); statErr == nil {
			if !info.Mode().IsRegular() {
				return fmt.Errorf("appearance archive is not a regular file: %s", file.Path)
			}
			// 后续统一扫描会校验现存归档，不重复解密和重写全部历史事件。
			continue
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return statErr
		}
		data, readErr := repo.openFile(file)
		if readErr != nil {
			return readErr
		}
		if err = repo.writeAppearanceImmutable(repo.absPath(file.Path), data); err != nil {
			return err
		}
	}
	return nil
}
