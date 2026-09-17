package dejavu

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/siyuan-note/dejavu/entity"
)

func (repo *Repo) legacyAppearanceArchive(key string, files []*entity.File, context map[string]interface{}) (*appearanceArchive, error) {
	ret := &appearanceArchive{Key: key, Files: map[string][]byte{}}
	digests := map[string]string{}
	for _, file := range files {
		if appearancePackageKey(file.Path) != key {
			continue
		}
		if err := repo.ensureFileChunks(file, context); err != nil {
			return nil, err
		}
		data, err := repo.openFile(file)
		if err != nil {
			return nil, err
		}
		if file.Path == appearanceRecordPath(key) {
			ret.State = data
			continue
		}
		rel := strings.TrimPrefix(file.Path, key+"/")
		ret.Files[rel] = data
		digest := sha256.Sum256(data)
		digests[rel] = hex.EncodeToString(digest[:])
	}
	if ret.State == nil {
		var err error
		ret.State, err = json.Marshal(map[string]interface{}{"version": 1, "deleted": false,
			"migration": true, "installTime": int64(0), "updateTime": int64(0), "files": digests})
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
}

func appearanceDeletedArchive(key string, source *appearanceArchive) (*appearanceArchive, error) {
	state := map[string]interface{}{}
	if source != nil {
		if err := json.Unmarshal(source.State, &state); err != nil {
			return nil, err
		}
	}
	state["version"], state["deleted"], state["migration"] = 1, true, false
	state["files"] = map[string]string{}
	data, err := json.Marshal(state)
	return &appearanceArchive{Key: key, State: data, Files: map[string][]byte{}}, err
}

// checkoutAppearanceSnapshot 将还原表示为新的因果事件，保留历史事件和删除记录。
func (repo *Repo) checkoutAppearanceSnapshot(id string, context map[string]interface{}) (upserts, removes []*entity.File, err error) {
	ignoreLines := repo.appearanceIgnoreLines
	defer func() { repo.appearanceIgnoreLines = ignoreLines }()
	if err = repo.checkAssetState(); err != nil {
		return
	}
	if err = repo.withAppearanceLock(func() error { return repo.ensureAllAssets(context) }); err != nil {
		return
	}
	unlock := repo.lockAppearance()
	defer func() {
		if unlock != nil {
			unlock()
		}
	}()
	index, err := repo.store.GetIndex(id)
	if err != nil {
		return
	}
	if index.ID != id || !index.VerifyAESKey(repo.store.AesKey) {
		return nil, nil, fmt.Errorf("invalid appearance snapshot index")
	}
	targetFiles, err := repo.getFiles(index.Files)
	if err != nil {
		return
	}
	if err = repo.selectAppearanceSnapshotIgnore(targetFiles, context); err != nil {
		return
	}
	for _, file := range targetFiles {
		if key := appearanceEventContainerKey(file.Path); key != "" && repo.appearanceIgnored(key) {
			continue
		}
		if err = repo.ensureFileChunks(file, context); err != nil {
			return
		}
	}
	versioned, err := repo.isAppearanceIndex(index, context)
	if err != nil {
		return
	}
	latest, err := repo.Latest()
	if err != nil {
		return
	}
	if _, err = repo.isAppearanceIndex(latest, context); err != nil {
		return
	}
	currentFiles, err := repo.getFiles(latest.Files)
	if err != nil {
		return
	}
	currentEvents, err := repo.readAppearanceEvents(currentFiles, context)
	if err != nil {
		return
	}
	targetEvents, err := repo.readAppearanceEvents(targetFiles, context)
	if err != nil {
		return
	}
	events, err := unionAppearanceEvents(currentEvents, targetEvents)
	if err != nil {
		return
	}
	legacy := appearancePackages(targetFiles)
	keys := map[string]bool{}
	for key := range events {
		keys[key] = true
	}
	for key := range legacy {
		keys[key] = true
	}
	for key := range keys {
		if repo.appearanceIgnored(key) {
			continue
		}
		previous := selectAppearanceEvent(currentEvents[key], nil)
		selected := selectAppearanceEvent(targetEvents[key], nil)
		var desired *appearanceArchive
		if selected != nil {
			desired = &appearanceArchive{Key: key, State: selected.State, Files: selected.Files}
		} else if legacy[key] != nil {
			desired, err = repo.legacyAppearanceArchive(key, targetFiles, context)
		} else if versioned && previous != nil {
			desired, err = appearanceDeletedArchive(key, previous.appearanceArchive)
		}
		if err != nil {
			return nil, nil, err
		}
		if desired == nil || previous != nil && sameAppearanceProjection(previous.appearanceArchive, desired) {
			continue
		}
		for _, head := range appearanceEventHeads(events[key]) {
			desired.Parents = append(desired.Parents, head.Digest)
		}
		data, encodeErr := encodeAppearanceArchive(desired)
		if encodeErr != nil {
			return nil, nil, encodeErr
		}
		file, putErr := repo.putAppearanceBytes(appearanceArchivePath(desired), data, true)
		if putErr != nil {
			return nil, nil, putErr
		}
		if events[key] == nil {
			events[key] = map[string]*appearanceEvent{}
		}
		events[key][desired.Digest] = &appearanceEvent{appearanceArchive: desired, File: file}
	}
	logical := map[string]*entity.File{}
	for _, file := range targetFiles {
		if repo.appearanceProtocolFileIgnored(file.Path) || appearancePackageKey(file.Path) != "" || appearanceEventContainerKey(file.Path) != "" {
			continue
		}
		logical[file.Path] = file
	}
	for key, packageEvents := range events {
		if repo.appearanceIgnored(key) {
			continue
		}
		for _, event := range packageEvents {
			logical[event.File.Path] = event.File
		}
	}
	for _, file := range currentFiles {
		if file.Path == appearanceFormatPath && !repo.appearanceProtocolIgnored() {
			logical[file.Path] = file
		}
	}
	physicalFiles, err := repo.walkSnapshotFiles(context)
	if err != nil {
		return nil, nil, err
	}
	upserts, removes = repo.diffUpsertRemove(mapAppearanceFiles(logical), physicalFiles, false)
	if err = repo.ensureAppearanceJournal(append(append([]*entity.File{}, currentFiles...), mapAppearanceFiles(logical)...)); err != nil {
		return nil, nil, err
	}
	if repo.assetDownloads == nil {
		if err = repo.checkoutFiles(upserts, context); err != nil {
			return
		}
		err = repo.removeFiles(removes, context)
		return
	}
	before, err := repo.appearanceBeforeProjectionFiles(currentFiles, context)
	if err != nil {
		return nil, nil, err
	}
	target, err := repo.appearanceProjectionFiles(mapAppearanceFiles(logical), context)
	if err != nil {
		return nil, nil, err
	}
	pending := &assetApply{Index: latest, Base: &entity.Index{}, Upserts: upserts, Removes: removes,
		Before: map[string]*entity.File{}, Deferred: map[string]*entity.File{}}
	current := filesByPath(currentFiles)
	for i, file := range pending.Removes {
		previous := current[file.Path]
		if previous == nil {
			return nil, nil, fmt.Errorf("%w: unindexed file during snapshot checkout: %s", ErrIndexFileChanged, file.Path)
		}
		pending.Removes[i] = previous
	}
	for _, file := range append(append([]*entity.File{}, upserts...), removes...) {
		pending.Before[file.Path] = current[file.Path]
	}
	for p, file := range target {
		if !equalFileContent(file, before[p]) {
			pending.Upserts = append(pending.Upserts, file)
			pending.Before[p] = before[p]
		}
	}
	for p, file := range before {
		if target[p] == nil && !repo.appearanceIgnored(appearancePackageKey(p)) {
			pending.Removes = append(pending.Removes, file)
			pending.Before[p] = file
		}
	}
	if err = repo.completeAppearanceApply(pending, target, before, context); err != nil {
		return nil, nil, err
	}
	if err = repo.preflightAppearanceApply(pending); err != nil {
		return nil, nil, err
	}
	repo.assetDownloads.state.Pending = pending
	if err = repo.saveAssetState(); err != nil {
		repo.assetDownloads.state.Pending = nil
		return nil, nil, err
	}
	unlock()
	unlock = nil
	err = repo.recoverAssetApply(context, false)
	return pending.Upserts, pending.Removes, err
}

// selectAppearanceSnapshotIgnore 使用目标快照中的用户规则，受管隔离块不改变分享选择。
func (repo *Repo) selectAppearanceSnapshotIgnore(files []*entity.File, context map[string]interface{}) error {
	if repo.ignoreRulePath == "" {
		return nil
	}
	var lines []string
	found := false
	for _, file := range files {
		if file.Path != repo.ignoreRulePath {
			continue
		}
		if found {
			return fmt.Errorf("duplicate snapshot ignore rule file")
		}
		found = true
		if err := repo.ensureFileChunks(file, context); err != nil {
			return err
		}
		data, err := repo.openFile(file)
		if err != nil {
			return err
		}
		lines, err = appearanceUserIgnoreLines(strings.Split(string(data), "\n"))
		if err != nil {
			return err
		}
	}
	repo.appearanceIgnoreLines = lines
	return nil
}
