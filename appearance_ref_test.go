package dejavu

import (
	"errors"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/siyuan-note/dejavu/cloud"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/dejavu/util"
)

type appearanceRefCloud struct {
	*cloud.Local
	mu          sync.Mutex
	uploads     []string
	downloads   []string
	failPrefix  string
	backupCount int
	statReads   int
	onTag       func() error
}

func (remote *appearanceRefCloud) UploadObject(p string, overwrite bool) (int64, error) {
	remote.mu.Lock()
	remote.uploads = append(remote.uploads, p)
	remote.mu.Unlock()
	if remote.failPrefix != "" && strings.HasPrefix(p, remote.failPrefix) {
		return 0, errors.New("injected appearance publication failure")
	}
	return remote.Local.UploadObject(p, overwrite)
}

func (remote *appearanceRefCloud) DownloadObject(p string) ([]byte, error) {
	remote.mu.Lock()
	remote.downloads = append(remote.downloads, p)
	remote.mu.Unlock()
	return remote.Local.DownloadObject(p)
}

func (remote *appearanceRefCloud) UploadBytes(p string, data []byte, overwrite bool) (int64, error) {
	remote.mu.Lock()
	remote.uploads = append(remote.uploads, p)
	remote.mu.Unlock()
	if p == "refs/tags/"+AppearanceRecoveryTag && remote.onTag != nil {
		if err := remote.onTag(); err != nil {
			return 0, err
		}
	}
	return remote.Local.UploadBytes(p, data, overwrite)
}

func (remote *appearanceRefCloud) GetStat() (*cloud.Stat, error) {
	remote.statReads++
	return &cloud.Stat{Sync: &cloud.StatSync{}, Backup: &cloud.StatBackup{Count: remote.backupCount}}, nil
}

func newAppearanceRefRepo(t *testing.T) (*Repo, *appearanceRefCloud) {
	t.Helper()
	repo, local := newChunkSourceTestRepo(t)
	repo.appearanceSyncEnabled = true
	remote := &appearanceRefCloud{Local: local}
	repo.cloud = remote
	return repo, remote
}

func appearanceRefIndex(t *testing.T, repo *Repo, note, marker string, events ...*appearanceArchive) *entity.Index {
	t.Helper()
	index := &entity.Index{ID: util.RandHash(), Memo: "appearance recovery fixture", Created: 1700000000000, SystemID: "fixture"}
	index.InitAESKeyVerifyVal(repo.store.AesKey)
	put := func(p string, data []byte, updated int64) {
		file := entity.NewFile(p, int64(len(data)), updated)
		file.Chunks = []string{util.Hash(data)}
		if err := repo.store.PutChunk(&entity.Chunk{ID: file.Chunks[0], Data: data}); err != nil {
			t.Fatal(err)
		}
		if err := repo.store.PutFile(file); err != nil {
			t.Fatal(err)
		}
		index.Files = append(index.Files, file.ID)
		index.Size += file.Size
	}
	put("/20260917000000-notebox/20260917000001-oldnote.sy", []byte(note), 1700000000000+int64(len(note))*1000)
	if marker != "" {
		put(appearanceFormatPath, []byte(marker), appearanceEventModified)
	}
	for _, event := range events {
		data, err := encodeAppearanceArchive(event)
		if err != nil {
			t.Fatal(err)
		}
		put(appearanceArchivePath(event), data, appearanceEventModified)
	}
	index.Count = len(index.Files)
	if err := repo.store.PutIndex(index); err != nil {
		t.Fatal(err)
	}
	return index
}

func appearanceRefEvent(t *testing.T, deleted bool, parents ...string) *appearanceArchive {
	t.Helper()
	files := map[string][]byte{}
	if !deleted {
		files["theme.css"] = []byte("body { color: black; }")
	}
	return &appearanceArchive{Key: "/themes/ref-test", Parents: parents, Files: files,
		State: appearanceArchiveTestState(t, files, deleted)}
}

func TestAppearanceRecoveryRootPublicationOrderAndQuota(t *testing.T) {
	repo, remote := newAppearanceRefRepo(t)
	event := appearanceRefEvent(t, false)
	index := appearanceRefIndex(t, repo, "complete ordinary notebook content", appearanceFormatData, event)
	remote.backupCount = 12
	remote.onTag = func() error {
		if _, err := remote.GetIndex(index.ID); err != nil {
			return err
		}
		files, err := repo.getFiles(index.Files)
		if err != nil {
			return err
		}
		for _, file := range files {
			for _, id := range append([]string{file.ID}, file.Chunks...) {
				if _, err = remote.DownloadObject(path.Join("objects", id[:2], id[2:])); err != nil {
					return err
				}
			}
		}
		return nil
	}
	stat, err := repo.publishAppearanceRecoveryRoot(index, map[string]interface{}{})
	if err != nil {
		t.Fatal(err)
	}
	if remote.statReads != 0 || stat.UploadChunkCount != 3 || stat.UploadFileCount != 5 {
		t.Fatalf("internal publication used backup quota or omitted ordinary objects: stat=%+v quotaReads=%d", stat, remote.statReads)
	}
	if last := remote.uploads[len(remote.uploads)-1]; last != "refs/tags/"+AppearanceRecoveryTag {
		t.Fatalf("tag was not the last publication: %v", remote.uploads)
	}
	for _, p := range remote.uploads {
		if strings.HasPrefix(p, "refs/latest") {
			t.Fatal("recovery publisher modified latest")
		}
	}
	root, _, err := repo.readAppearanceRecoveryRoot(map[string]interface{}{})
	if err != nil || root.ID != index.ID || !reflect.DeepEqual(root.Files, index.Files) {
		t.Fatalf("recovery tag does not reference the complete normal snapshot: %+v %v", root, err)
	}
	before := len(remote.uploads)
	noteOnly := appearanceRefIndex(t, repo, "changed ordinary notebook content only", appearanceFormatData, event)
	if _, err = repo.publishAppearanceRecoveryRoot(noteOnly, map[string]interface{}{}); err != nil {
		t.Fatal(err)
	}
	if len(remote.uploads) != before {
		t.Fatal("ordinary note changes republished unchanged appearance events")
	}
}

func TestAppearanceRecoveryRootKeepsPreviousTagOnUploadFailure(t *testing.T) {
	for _, fail := range []string{"objects/", "indexes/"} {
		t.Run(fail, func(t *testing.T) {
			repo, remote := newAppearanceRefRepo(t)
			initialEvent := appearanceRefEvent(t, false)
			initial := appearanceRefIndex(t, repo, "initial", appearanceFormatData, initialEvent)
			if _, err := repo.publishAppearanceRecoveryRoot(initial, map[string]interface{}{}); err != nil {
				t.Fatal(err)
			}
			deleted := appearanceRefEvent(t, true, initialEvent.Digest)
			next := appearanceRefIndex(t, repo, "updated note", appearanceFormatData, initialEvent, deleted)
			remote.failPrefix = fail
			if _, err := repo.publishAppearanceRecoveryRoot(next, map[string]interface{}{}); err == nil {
				t.Fatal("expected publication failure")
			}
			data, err := remote.DownloadObject("refs/tags/" + AppearanceRecoveryTag)
			if err != nil || string(data) != initial.ID {
				t.Fatalf("failed publication replaced prior root: %q %v", data, err)
			}
		})
	}
}

func TestAppearanceRecoveryTagGuardsAndLists(t *testing.T) {
	repo, remote := newAppearanceRefRepo(t)
	index := appearanceRefIndex(t, repo, "note", appearanceFormatData, appearanceRefEvent(t, false))
	if _, err := repo.publishAppearanceRecoveryRoot(index, map[string]interface{}{}); err != nil {
		t.Fatal(err)
	}
	for name, call := range map[string]func() error{
		"replace local": func() error { return repo.AddTag(index.ID, AppearanceRecoveryTag) },
		"remove local":  func() error { return repo.RemoveTag(AppearanceRecoveryTag) },
		"replace cloud": func() error {
			_, _, _, err := repo.UploadTagIndex(AppearanceRecoveryTag, index.ID, map[string]interface{}{})
			return err
		},
		"remove cloud": func() error { return repo.RemoveCloudRepoTag(AppearanceRecoveryTag) },
	} {
		if err := call(); !errors.Is(err, ErrAppearanceRecoveryTag) {
			t.Fatalf("%s did not protect internal tag: %v", name, err)
		}
	}
	localTags, err := repo.GetTagLogs()
	if err != nil || len(localTags) != 0 {
		t.Fatalf("internal local tag was exposed: %+v %v", localTags, err)
	}
	cloudTags, err := repo.GetCloudRepoTagLogs(map[string]interface{}{})
	if err != nil || len(cloudTags) != 0 {
		t.Fatalf("internal cloud tag was exposed: %+v %v", cloudTags, err)
	}
	remote.backupCount = 12
	stat, err := repo.GetCloudRepoStat()
	if err != nil || stat.Backup.Count != 11 {
		t.Fatalf("internal tag consumed a user backup slot: %+v %v", stat, err)
	}
	if err = repo.AddTag(index.ID, "twelfth-user-backup"); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err = repo.UploadTagIndex("twelfth-user-backup", index.ID, map[string]interface{}{}); err != nil {
		t.Fatalf("internal tag prevented the twelfth user backup: %v", err)
	}
}

func TestAppearanceRecoveryTagPreservesSameNamedUserTag(t *testing.T) {
	for _, marker := range []string{"", strings.Repeat("x", len(appearanceFormatData))} {
		t.Run(marker, func(t *testing.T) {
			repo, remote := newAppearanceRefRepo(t)
			user := appearanceRefIndex(t, repo, "ordinary user snapshot", marker)
			if err := repo.AddTag(user.ID, AppearanceRecoveryTag); err != nil {
				t.Fatalf("existing user tag name was rejected: %v", err)
			}
			if err := os.MkdirAll(filepath.Join(remote.GetConf().Local.Endpoint, "main", "refs"), 0755); err != nil {
				t.Fatal(err)
			}
			if _, _, _, err := repo.UploadTagIndex(AppearanceRecoveryTag, user.ID, map[string]interface{}{}); err != nil {
				t.Fatal(err)
			}
			localRef := filepath.Join(repo.Path, "refs", "tags", AppearanceRecoveryTag)
			beforeLocal, _ := os.ReadFile(localRef)
			beforeCloud, _ := remote.DownloadObject("refs/tags/" + AppearanceRecoveryTag)
			other, _ := newAppearanceRefRepo(t)
			owned := appearanceRefIndex(t, other, "protocol snapshot", appearanceFormatData, appearanceRefEvent(t, false))
			other.cloud = remote
			if _, err := other.publishAppearanceRecoveryRoot(owned, map[string]interface{}{}); !errors.Is(err, ErrAppearanceRecoveryTagConflict) {
				t.Fatalf("user tag was claimed by protocol: %v", err)
			}
			afterLocal, _ := os.ReadFile(localRef)
			afterCloud, _ := remote.DownloadObject("refs/tags/" + AppearanceRecoveryTag)
			if string(beforeLocal) != string(afterLocal) || string(beforeCloud) != string(afterCloud) {
				t.Fatal("protocol publication changed same-named user refs")
			}
			localTags, err := repo.GetTagLogs()
			if err != nil || len(localTags) != 1 || localTags[0].Tag != AppearanceRecoveryTag {
				t.Fatalf("same-named user local tag was hidden: %+v %v", localTags, err)
			}
			cloudTags, err := repo.GetCloudRepoTagLogs(map[string]interface{}{})
			if err != nil || len(cloudTags) != 1 || cloudTags[0].Tag != AppearanceRecoveryTag {
				t.Fatalf("same-named user cloud tag was hidden: %+v %v", cloudTags, err)
			}
			remote.backupCount = 12
			stat, err := repo.GetCloudRepoStat()
			if err != nil || stat.Backup.Count != 12 {
				t.Fatalf("user backup count was discounted: %+v %v", stat, err)
			}
			if err = repo.RemoveTag(AppearanceRecoveryTag); err != nil {
				t.Fatalf("user local tag became undeletable: %v", err)
			}
			if err = repo.RemoveCloudRepoTag(AppearanceRecoveryTag); err != nil {
				t.Fatalf("user cloud tag became undeletable: %v", err)
			}
		})
	}
}

func TestAppearanceRecoveryRootRejectsMissingParentsAndEventLoss(t *testing.T) {
	repo, _ := newAppearanceRefRepo(t)
	initialEvent := appearanceRefEvent(t, false)
	initial := appearanceRefIndex(t, repo, "initial", appearanceFormatData, initialEvent)
	if err := repo.rememberAppearanceRecoveryRoot(initial); err != nil {
		t.Fatal(err)
	}
	missingParent := appearanceRefEvent(t, true, strings.Repeat("a", 64))
	invalid := appearanceRefIndex(t, repo, "invalid", appearanceFormatData, initialEvent, missingParent)
	if err := repo.rememberAppearanceRecoveryRoot(invalid); err == nil {
		t.Fatal("accepted an incomplete event graph")
	}
	dropped := appearanceRefIndex(t, repo, "event removed", appearanceFormatData)
	if err := repo.rememberAppearanceRecoveryRoot(dropped); err != nil {
		t.Fatalf("stopping sharing blocked ordinary snapshots: %v", err)
	}
	id, err := repo.GetTag(AppearanceRecoveryTag)
	if err != nil || id != initial.ID {
		t.Fatalf("invalid root replaced retained state: %s %v", id, err)
	}
}

func TestAppearanceRecoveryRootDoesNotReplaceLocalUserTag(t *testing.T) {
	repo, _ := newAppearanceRefRepo(t)
	user := appearanceRefIndex(t, repo, "existing user snapshot", "")
	if err := repo.AddTag(user.ID, AppearanceRecoveryTag); err != nil {
		t.Fatal(err)
	}
	owned := appearanceRefIndex(t, repo, "new appearance snapshot", appearanceFormatData, appearanceRefEvent(t, false))
	if err := repo.rememberAppearanceRecoveryRoot(owned); !errors.Is(err, ErrAppearanceRecoveryTagConflict) {
		t.Fatalf("expected local user tag collision, got %v", err)
	}
	id, err := repo.GetTag(AppearanceRecoveryTag)
	if err != nil || id != user.ID {
		t.Fatalf("local user tag was replaced: %s %v", id, err)
	}
	if err = repo.RemoveTag(AppearanceRecoveryTag); err != nil {
		t.Fatalf("local user tag was locked after collision: %v", err)
	}
}

func TestAppearanceRecoveryDisabledPreservesOrdinaryTagBehavior(t *testing.T) {
	repo, remote := newAppearanceRefRepo(t)
	repo.appearanceSyncEnabled = false
	index := appearanceRefIndex(t, repo, "note", appearanceFormatData, appearanceRefEvent(t, false))
	if err := repo.rememberAppearanceRecoveryRoot(index); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.publishAppearanceRecoveryRoot(index, map[string]interface{}{}); err != nil || len(remote.uploads) != 0 {
		t.Fatalf("disabled appearance published state: %v %v", remote.uploads, err)
	}
	if err := repo.AddTag(index.ID, AppearanceRecoveryTag); err != nil {
		t.Fatal(err)
	}
	tags, err := repo.GetTagLogs()
	if err != nil || len(tags) != 1 {
		t.Fatalf("disabled appearance changed tag visibility: %+v %v", tags, err)
	}
	if err = repo.RemoveTag(AppearanceRecoveryTag); err != nil {
		t.Fatal(err)
	}
}

func TestAppearanceRecoveryRootNeverPublishesIgnoredPrivateEvents(t *testing.T) {
	repo, remote := newAppearanceRefRepo(t)
	installed := appearanceRefEvent(t, false)
	initial := appearanceRefIndex(t, repo, "first note", appearanceFormatData, installed)
	if _, err := repo.publishAppearanceRecoveryRoot(initial, map[string]interface{}{}); err != nil {
		t.Fatal(err)
	}
	deleted := appearanceRefEvent(t, true, installed.Digest)
	public := appearanceRefIndex(t, repo, "public deletion note", appearanceFormatData, installed, deleted)
	if _, err := repo.publishAppearanceRecoveryRoot(public, map[string]interface{}{}); err != nil {
		t.Fatal(err)
	}
	private := appearanceRefEvent(t, false, deleted.Digest)
	private.Files["theme.css"] = []byte("body { color: private; }")
	private.State = appearanceArchiveTestState(t, private.Files, false)
	privateIndex := appearanceRefIndex(t, repo, "never shared note", appearanceFormatData, installed, deleted, private)
	if err := repo.rememberAppearanceRecoveryRoot(privateIndex); err != nil {
		t.Fatal(err)
	}
	repo.appearanceIgnoreLines = []string{"/themes/ref-test"}
	ignored := appearanceRefIndex(t, repo, "note with ignored package", appearanceFormatData)
	before := len(remote.uploads)
	if _, err := repo.publishAppearanceRecoveryRoot(ignored, map[string]interface{}{}); err != nil {
		t.Fatal(err)
	}
	if len(remote.uploads) != before {
		t.Fatal("whole-package ignore republished the recovery root")
	}
	shared := appearanceRefEvent(t, false)
	shared.Key = "/themes/shared-second"
	target := appearanceRefIndex(t, repo, "new ordinary note and shared package", appearanceFormatData, shared, private)
	if _, err := repo.publishAppearanceRecoveryRoot(target, map[string]interface{}{}); err != nil {
		t.Fatal(err)
	}
	root, _, err := repo.readAppearanceRecoveryRoot(map[string]interface{}{})
	if err != nil {
		t.Fatal(err)
	}
	rootFiles, err := repo.getFiles(root.Files)
	if err != nil {
		t.Fatal(err)
	}
	paths := map[string]bool{}
	for _, file := range rootFiles {
		paths[file.Path] = true
	}
	if !paths[appearanceArchivePath(installed)] || !paths[appearanceArchivePath(deleted)] || !paths[appearanceArchivePath(shared)] {
		t.Fatal("new cloud recovery root dropped a published event or tombstone")
	}
	if paths[appearanceArchivePath(private)] {
		t.Fatal("private ignored event leaked into cloud recovery index")
	}
	privateFile := entity.NewFile(appearanceArchivePath(private), 0, appearanceEventModified)
	storedPrivate, err := repo.store.GetFile(privateFile.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range append([]string{storedPrivate.ID}, storedPrivate.Chunks...) {
		if _, err = remote.DownloadObject(path.Join("objects", id[:2], id[2:])); !errors.Is(err, cloud.ErrCloudObjectNotFound) {
			t.Fatalf("private ignored object was uploaded: %s %v", id, err)
		}
	}
	localID, err := repo.GetTag(AppearanceRecoveryTag)
	if err != nil {
		t.Fatal(err)
	}
	localIndex, err := repo.GetIndex(localID)
	if err != nil {
		t.Fatal(err)
	}
	retainedPrivate := false
	for _, id := range localIndex.Files {
		retainedPrivate = retainedPrivate || id == storedPrivate.ID
	}
	if !retainedPrivate {
		t.Fatal("local recovery root discarded the unshared event")
	}
	if len(root.Files) != 5 {
		t.Fatalf("cloud recovery root is not the full ordinary snapshot plus published events: %+v", root)
	}
}

func TestAppearanceRecoveryRootPreservesIgnoredOpaqueHistory(t *testing.T) {
	repo, remote := newAppearanceRefRepo(t)
	installed := appearanceRefEvent(t, false)
	initial := appearanceRefIndex(t, repo, "ordinary note", appearanceFormatData, installed)
	if _, err := repo.publishAppearanceRecoveryRoot(initial, nil); err != nil {
		t.Fatal(err)
	}
	deleted := appearanceRefEvent(t, true, installed.Digest)
	public := appearanceRefIndex(t, repo, "ordinary note with deletion", appearanceFormatData, installed, deleted)
	if _, err := repo.publishAppearanceRecoveryRoot(public, nil); err != nil {
		t.Fatal(err)
	}
	// 模拟旧端运输的未知包文件和已公开但损坏的整包归档，缺失负载也只保留引用。
	broken, brokenChunks := appearanceSourceFile("/themes/ref-test", "not a package archive")
	unknown := entity.NewFile("/storage/appearance-v1/themes/ref-test/future.format", 19, appearanceEventModified)
	unknown.Chunks = []string{util.Hash([]byte("unavailable payload"))}
	objects := encodeAppearanceSourceFile(t, repo, broken, brokenChunks)
	for id, data := range encodeAppearanceSourceFile(t, repo, unknown, nil) {
		objects[id] = data
	}
	for id, data := range objects {
		if _, err := remote.Local.UploadBytes(path.Join("objects", id[:2], id[2:]), data, true); err != nil {
			t.Fatal(err)
		}
	}
	for _, file := range []*entity.File{broken, unknown} {
		public.Files = append(public.Files, file.ID)
		public.Size += file.Size
	}
	public.ID, public.Count = util.RandHash(), len(public.Files)
	if err := repo.store.PutIndex(public); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.uploadIndex(public, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := remote.Local.UploadBytes("refs/tags/"+AppearanceRecoveryTag, []byte(public.ID), true); err != nil {
		t.Fatal(err)
	}
	receiver, _ := newAppearanceRefRepo(t)
	receiver.cloud = remote
	remote.GetConf().RepoPath = receiver.Path
	receiver.appearanceIgnoreLines = []string{"/themes/ref-test/"}
	source := &testChunkSource{chunks: objects}
	receiver.SetChunkSource(source)
	root, _, err := receiver.readAppearanceRecoveryRoot(nil)
	if err != nil || root.ID != public.ID {
		t.Fatalf("ignored recovery payload blocked note sync: %+v %v", root, err)
	}
	files, err := receiver.getFiles(root.Files)
	if err != nil {
		t.Fatal(err)
	}
	events, err := receiver.readAppearanceEvents(files, nil)
	if err != nil || len(events) != 0 {
		t.Fatalf("ignored history was parsed or selected for application: %+v %v", events, err)
	}
	shared := appearanceRefEvent(t, false)
	shared.Key = "/icons/still-shared"
	target := appearanceRefIndex(t, receiver, "latest ordinary note", appearanceFormatData, shared)
	if _, err = receiver.publishAppearanceRecoveryRoot(target, nil); err != nil {
		t.Fatalf("ignored unavailable history blocked unrelated package publication: %v", err)
	}
	after, _, err := receiver.readAppearanceRecoveryRoot(nil)
	if err != nil {
		t.Fatal(err)
	}
	retained := map[string]bool{}
	for _, id := range after.Files {
		retained[id] = true
	}
	for _, file := range files {
		if !receiver.appearanceProtocolFileIgnored(file.Path) {
			continue
		}
		if !retained[file.ID] {
			t.Fatalf("ignored public tombstone or opaque history lost its reference: %s", file.Path)
		}
		for _, id := range file.Chunks {
			if _, err = receiver.store.Stat(id); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("ignored payload was downloaded: %s %v", file.Path, err)
			}
			if _, requested := source.downloads.Load(id); requested {
				t.Fatalf("ignored payload was requested from LAN: %s", file.Path)
			}
			key := path.Join("objects", id[:2], id[2:])
			for _, downloaded := range remote.downloads {
				if downloaded == key {
					t.Fatalf("ignored payload was requested from cloud: %s", file.Path)
				}
			}
		}
	}
}

func TestAppearanceRecoveryRootIgnoreKeepsMetadataBoundary(t *testing.T) {
	for _, p := range []string{
		"/storage/appearance-v1/themes/ref-test/../outside.future",
		"/storage/appearance-v1/unscoped.future",
		"/storage/appearance-v1/themes/not-ignored/future.format",
	} {
		t.Run(p, func(t *testing.T) {
			repo, _ := newAppearanceRefRepo(t)
			repo.appearanceIgnoreLines = []string{"/themes/ref-test/"}
			index := appearanceRefIndex(t, repo, "ordinary note", appearanceFormatData)
			file := entity.NewFile(p, 1, appearanceEventModified)
			file.Chunks = []string{util.Hash([]byte("x"))}
			if err := repo.store.PutFile(file); err != nil {
				t.Fatal(err)
			}
			index.Files = append(index.Files, file.ID)
			if err := repo.validateAppearanceRecoveryIndex(index, false, nil, newAppearanceTraffic()); err == nil {
				t.Fatal("package ignore bypassed protocol path boundaries")
			}
		})
	}
}

func TestAppearanceRecoveryRootAuthenticatesRemoteMarker(t *testing.T) {
	repo, remote := newAppearanceRefRepo(t)
	index := appearanceRefIndex(t, repo, "ordinary note", appearanceFormatData, appearanceRefEvent(t, false))
	if _, err := repo.publishAppearanceRecoveryRoot(index, map[string]interface{}{}); err != nil {
		t.Fatal(err)
	}
	marker := entity.NewFile(appearanceFormatPath, int64(len(appearanceFormatData)), appearanceEventModified)
	key := path.Join("objects", marker.ID[:2], marker.ID[2:])
	corrupted, err := remote.DownloadObject(key)
	if err != nil {
		t.Fatal(err)
	}
	corrupted[len(corrupted)-1] ^= 1
	if _, err = remote.Local.UploadBytes(key, corrupted, true); err != nil {
		t.Fatal(err)
	}
	if _, _, err = repo.readAppearanceRecoveryRoot(map[string]interface{}{}); err == nil {
		t.Fatal("local marker cache bypassed remote authentication failure")
	}
	after, err := remote.DownloadObject(key)
	if err != nil || !reflect.DeepEqual(after, corrupted) {
		t.Fatal("failed authentication modified the original cloud object")
	}
}

func TestAppearanceRecoveryRootReusesAndCountsSourceChunks(t *testing.T) {
	repo, remote := newAppearanceRefRepo(t)
	event := appearanceRefEvent(t, false)
	index := appearanceRefIndex(t, repo, "ordinary note", appearanceFormatData, event)
	if _, err := repo.publishAppearanceRecoveryRoot(index, map[string]interface{}{}); err != nil {
		t.Fatal(err)
	}
	files, err := repo.getFiles(index.Files)
	if err != nil {
		t.Fatal(err)
	}
	objects := map[string][]byte{}
	for _, file := range files {
		for _, id := range append([]string{file.ID}, file.Chunks...) {
			objects[id], err = remote.DownloadObject(path.Join("objects", id[:2], id[2:]))
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	receiver, _ := newAppearanceRefRepo(t)
	receiver.cloud = remote
	source := &testChunkSource{chunks: objects}
	receiver.SetChunkSource(source)
	root, stat, err := receiver.readAppearanceRecoveryRoot(map[string]interface{}{})
	if err != nil || root.ID != index.ID || stat.DownloadChunkCount != 2 || stat.PeerDownloadChunkCount != 2 ||
		stat.PeerDownloadFileCount != 2 {
		t.Fatalf("source archive prefetch was omitted from recovery traffic: root=%+v stat=%+v err=%v", root, stat, err)
	}
	for _, file := range files {
		if file.Path != appearanceArchivePath(event) && file.Path != appearanceFormatPath {
			continue
		}
		for _, id := range file.Chunks {
			requests, ok := source.downloads.Load(id)
			if !ok || requests.(*atomic.Int32).Load() != 1 {
				t.Fatalf("recovery downloaded a verified chunk repeatedly: %s %+v", id, requests)
			}
		}
	}
}
