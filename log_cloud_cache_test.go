package dejavu

import (
	"crypto/sha256"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/siyuan-note/dejavu/cloud"
	"github.com/siyuan-note/dejavu/entity"
)

type snapshotS3Object struct {
	data     []byte
	etag     string
	modified time.Time
}

type snapshotS3Request struct {
	key, conditional string
}

type snapshotS3Fixture struct {
	mu                sync.Mutex
	objects           map[string]snapshotS3Object
	requests          []snapshotS3Request
	listLimit         int
	listStatus        int
	conditionalStatus int
	getStatus         map[string]int
	gateKey           string
	entered           chan struct{}
	release           chan struct{}
	encoder           *zstd.Encoder
	key               []byte
	revision          int
}

func newSnapshotS3Fixture(t *testing.T) (*snapshotS3Fixture, *Repo) {
	t.Helper()
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	f := &snapshotS3Fixture{objects: map[string]snapshotS3Object{}, getStatus: map[string]int{}, encoder: encoder,
		key: []byte("01234567890123456789012345678901")}
	server := httptest.NewServer(http.HandlerFunc(f.serveHTTP))
	t.Cleanup(server.Close)
	t.Cleanup(func() { encoder.Close() })
	base := t.TempDir()
	s3 := cloud.NewS3(&cloud.BaseCloud{Conf: &cloud.Conf{S3: &cloud.ConfS3{
		Endpoint: server.URL, Bucket: "bucket", Region: "us-east-1", AccessKey: "access", SecretKey: "secret",
		PathStyle: true, Timeout: 10,
	}}}, server.Client())
	repo, err := NewRepo(filepath.Join(base, "data"), filepath.Join(base, "repo"), filepath.Join(base, "history"),
		filepath.Join(base, "temp"), "device", "Device", "test", f.key, nil, s3)
	if err != nil {
		t.Fatal(err)
	}
	return f, repo
}

func (f *snapshotS3Fixture) put(key string, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.revision++
	f.objects[key] = snapshotS3Object{data: data, etag: fmt.Sprintf("\"%x\"", sha256.Sum256(data)),
		modified: time.Date(2026, 10, 27, 0, 0, f.revision, 0, time.UTC)}
}

func snapshotTestID(i int) string { return fmt.Sprintf("%040x", i) }

func (f *snapshotS3Fixture) index(i int, memo string) {
	index := &entity.Index{ID: snapshotTestID(i), Created: int64(i), Memo: memo, Count: 3, Size: 2048,
		SystemID: "device", SystemName: "Device", SystemOS: "test", Files: []string{"file"}}
	index.InitAESKeyVerifyVal(f.key)
	data, _ := json.Marshal(index)
	f.put("indexes/"+index.ID, f.encoder.EncodeAll(data, nil))
}

func (f *snapshotS3Fixture) manifest(ids ...int) {
	list := &cloud.Indexes{Indexes: []*cloud.Index{}}
	for _, i := range ids {
		list.Indexes = append(list.Indexes, &cloud.Index{ID: snapshotTestID(i)})
	}
	data, _ := json.Marshal(list)
	f.put("indexes-v2.json", f.encoder.EncodeAll(data, nil))
}

func (f *snapshotS3Fixture) serveHTTP(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/bucket/repo/")
	prefix := strings.TrimPrefix(r.URL.Query().Get("prefix"), "repo/")
	if prefix != "" {
		key = "LIST:" + prefix
	}
	f.mu.Lock()
	f.requests = append(f.requests, snapshotS3Request{key, r.Header.Get("If-None-Match")})
	if key == f.gateKey && f.entered != nil {
		entered, release := f.entered, f.release
		f.entered = nil
		close(entered)
		f.mu.Unlock()
		<-release
		f.mu.Lock()
	}
	defer f.mu.Unlock()
	if prefix != "" {
		if f.listStatus != 0 {
			w.WriteHeader(f.listStatus)
			return
		}
		keys := []string{}
		for k := range f.objects {
			if strings.HasPrefix(k, prefix) && "repo/"+k > r.URL.Query().Get("marker") {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		limit, _ := strconv.Atoi(r.URL.Query().Get("max-keys"))
		if limit < 1 {
			limit = 1000
		}
		if f.listLimit > 0 && f.listLimit < limit {
			limit = f.listLimit
		}
		type entry struct {
			Key          string
			ETag         string
			Size         int
			LastModified string
		}
		output := struct {
			XMLName     xml.Name `xml:"ListBucketResult"`
			Xmlns       string   `xml:"xmlns,attr"`
			IsTruncated bool
			NextMarker  string `xml:",omitempty"`
			Contents    []entry
		}{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/", IsTruncated: len(keys) > limit}
		if len(keys) > limit {
			keys = keys[:limit]
			output.NextMarker = "repo/" + keys[len(keys)-1]
		}
		for _, k := range keys {
			obj := f.objects[k]
			output.Contents = append(output.Contents, entry{"repo/" + k, obj.etag, len(obj.data), obj.modified.Format(time.RFC3339)})
		}
		w.Header().Set("Content-Type", "application/xml")
		_ = xml.NewEncoder(w).Encode(output)
		return
	}
	if status := f.getStatus[key]; status != 0 {
		w.WriteHeader(status)
		return
	}
	obj, exists := f.objects[key]
	if !exists {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if r.Header.Get("If-None-Match") != "" {
		if f.conditionalStatus != 0 {
			w.WriteHeader(f.conditionalStatus)
			return
		}
		if r.Header.Get("If-None-Match") == obj.etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}
	w.Header().Set("ETag", obj.etag)
	w.Header().Set("Last-Modified", obj.modified.Format(http.TimeFormat))
	w.Header().Set("Content-Length", strconv.Itoa(len(obj.data)))
	_, _ = w.Write(obj.data)
}

func (f *snapshotS3Fixture) takeRequests() []snapshotS3Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	ret := f.requests
	f.requests = nil
	return ret
}

func loadSnapshotPage(t *testing.T, repo *Repo, cache *CloudSnapshotCache, page int) ([]*Log, int, int) {
	t.Helper()
	load, err := repo.NewCloudRepoLogLoader(cache)
	if err != nil {
		t.Fatal(err)
	}
	logs, pages, total, err := load(page)
	if err != nil {
		t.Fatal(err)
	}
	return logs, pages, total
}

func TestCloudSnapshotCachePaginationAndRevalidation(t *testing.T) {
	f, repo := newSnapshotS3Fixture(t)
	ids := []int{}
	for i := 1; i <= 65; i++ {
		f.index(i, "memo")
		ids = append(ids, i)
	}
	f.manifest(ids...)
	cache := &CloudSnapshotCache{}
	logs, pages, total := loadSnapshotPage(t, repo, cache, 1)
	if len(logs) != 32 || pages != 3 || total != 65 || logs[0].Files != nil || logs[0].HCreated == "" || logs[0].HSize == "" {
		t.Fatalf("unexpected first page: %d %d %d %+v", len(logs), pages, total, logs[0])
	}
	if calls := f.takeRequests(); len(calls) != 34 {
		t.Fatalf("cold page requests: %v", calls)
	}
	logs[0].Memo = "caller mutation"
	logs, _, _ = loadSnapshotPage(t, repo, cache, 1)
	if logs[0].Memo != "memo" {
		t.Fatal("caller mutated cached metadata")
	}
	if calls := f.takeRequests(); len(calls) != 2 || calls[0].conditional == "" {
		t.Fatalf("warm requests: %v", calls)
	}
	load, err := repo.NewCloudRepoLogLoader(cache)
	if err != nil {
		t.Fatal(err)
	}
	for page := 1; page <= 3; page++ {
		if _, _, _, err = load(page); err != nil {
			t.Fatal(err)
		}
	}
	if calls := f.takeRequests(); len(calls) != 35 {
		t.Fatalf("one manifest and 33 unfilled indexes expected: %v", calls)
	}
	load, err = repo.NewCloudRepoLogLoader(cache)
	if err != nil {
		t.Fatal(err)
	}
	for page := 1; page <= 3; page++ {
		if _, _, _, err = load(page); err != nil {
			t.Fatal(err)
		}
	}
	if calls := f.takeRequests(); len(calls) != 2 {
		t.Fatalf("warm full scan requests: %v", calls)
	}
}

func TestCloudSnapshotCacheMetadataAndManifestChanges(t *testing.T) {
	f, repo := newSnapshotS3Fixture(t)
	f.index(1, "before")
	f.manifest(1)
	cache := &CloudSnapshotCache{}
	loadSnapshotPage(t, repo, cache, 1)
	f.takeRequests()
	f.index(1, "after")
	logs, _, _ := loadSnapshotPage(t, repo, cache, 1)
	if logs[0].Memo != "after" {
		t.Fatal("same-ID memo update was hidden")
	}
	if calls := f.takeRequests(); len(calls) != 3 {
		t.Fatalf("only changed index should be fetched: %v", calls)
	}
	f.index(2, "new")
	f.manifest(2, 1)
	logs, _, total := loadSnapshotPage(t, repo, cache, 1)
	if total != 2 || logs[0].ID != snapshotTestID(2) {
		t.Fatalf("new snapshot not visible: %+v", logs)
	}
	if calls := f.takeRequests(); len(calls) != 3 {
		t.Fatalf("incremental requests: %v", calls)
	}
	f.manifest(1, 2)
	logs, _, _ = loadSnapshotPage(t, repo, cache, 1)
	if logs[0].ID != snapshotTestID(1) {
		t.Fatal("manifest ordering was cached as a set")
	}
	if calls := f.takeRequests(); len(calls) != 2 {
		t.Fatalf("same IDs should reuse metadata: %v", calls)
	}
	f.manifest(2)
	logs, _, total = loadSnapshotPage(t, repo, cache, 1)
	if total != 1 || len(logs) != 1 || logs[0].ID != snapshotTestID(2) {
		t.Fatal("removed snapshot remained visible")
	}
}

func TestCloudSnapshotCacheMissingAndFailedObjects(t *testing.T) {
	f, repo := newSnapshotS3Fixture(t)
	f.manifest(1)
	cache := &CloudSnapshotCache{}
	logs, _, _ := loadSnapshotPage(t, repo, cache, 1)
	if len(logs) != 0 {
		t.Fatal("missing index appeared")
	}
	f.index(1, "arrived")
	logs, _, _ = loadSnapshotPage(t, repo, cache, 1)
	if len(logs) != 1 || logs[0].Memo != "arrived" {
		t.Fatal("missing index was negatively cached")
	}
	f.index(1, "changed")
	f.mu.Lock()
	f.getStatus["indexes/"+snapshotTestID(1)] = http.StatusForbidden
	f.mu.Unlock()
	load, err := repo.NewCloudRepoLogLoader(cache)
	if err != nil {
		t.Fatal(err)
	}
	if logs, _, _, err = load(1); err == nil || len(logs) != 0 {
		t.Fatal("failed refresh served stale metadata")
	}
	f.mu.Lock()
	delete(f.getStatus, "indexes/"+snapshotTestID(1))
	f.mu.Unlock()
	logs, _, _ = loadSnapshotPage(t, repo, cache, 1)
	if logs[0].Memo != "changed" {
		t.Fatal("failed object poisoned cache")
	}
	f.put("indexes-v2.json", []byte("corrupt"))
	if _, err = repo.NewCloudRepoLogLoader(cache); err == nil {
		t.Fatal("corrupt manifest accepted")
	}
	f.manifest(1)
	logs, _, _ = loadSnapshotPage(t, repo, cache, 1)
	if logs[0].Memo != "changed" {
		t.Fatal("manifest recovery failed")
	}
}

func TestCloudSnapshotCacheFallbacks(t *testing.T) {
	for _, mode := range []string{"conditional", "missing-etag", "listing"} {
		t.Run(mode, func(t *testing.T) {
			f, repo := newSnapshotS3Fixture(t)
			f.index(1, "before")
			f.manifest(1)
			cache := &CloudSnapshotCache{}
			loadSnapshotPage(t, repo, cache, 1)
			f.takeRequests()
			f.index(1, "after")
			f.mu.Lock()
			switch mode {
			case "conditional":
				f.conditionalStatus = http.StatusNotImplemented
			case "missing-etag":
				for key, obj := range f.objects {
					obj.etag = ""
					f.objects[key] = obj
				}
			case "listing":
				f.listStatus = http.StatusForbidden
			}
			f.mu.Unlock()
			logs, _, _ := loadSnapshotPage(t, repo, cache, 1)
			if logs[0].Memo != "after" {
				t.Fatal("fallback served stale data")
			}
			calls := f.takeRequests()
			if mode == "conditional" && (len(calls) != 4 || calls[1].conditional != "") {
				t.Fatalf("unconditional retry missing: %v", calls)
			}
			loadSnapshotPage(t, repo, cache, 1)
			calls = f.takeRequests()
			if mode != "conditional" && len(calls) != 3 {
				t.Fatalf("unverified object was reused: %v", calls)
			}
		})
	}
}

func TestCloudSnapshotCacheTagChangesAndIsolation(t *testing.T) {
	f, repo := newSnapshotS3Fixture(t)
	f.index(1, "first")
	f.index(2, "second")
	f.put("refs/tags/a", []byte(snapshotTestID(1)))
	f.put("refs/tags/b", []byte(snapshotTestID(1)))
	cache := &CloudSnapshotCache{}
	load := func() []*Log {
		t.Helper()
		logs, err := repo.GetCloudRepoTagLogsCached(cache, nil)
		if err != nil {
			t.Fatal(err)
		}
		return logs
	}
	logs := load()
	if len(logs) != 2 || logs[0].Tag != "a" || logs[1].Tag != "b" || logs[0].HTagUpdated == "" {
		t.Fatalf("tag fields: %+v", logs)
	}
	if calls := f.takeRequests(); len(calls) != 5 {
		t.Fatalf("tags sharing an index fetched it twice: %v", calls)
	}
	logs[0].Tag = "mutated"
	logs = load()
	if logs[0].Tag != "a" {
		t.Fatal("returned tag log shared with cache")
	}
	if calls := f.takeRequests(); len(calls) != 2 {
		t.Fatalf("warm tag requests: %v", calls)
	}
	f.index(1, "edited")
	logs = load()
	if logs[0].Memo != "edited" || logs[1].Memo != "edited" {
		t.Fatal("unchanged refs concealed index update")
	}
	if calls := f.takeRequests(); len(calls) != 3 {
		t.Fatalf("tag index refresh requests: %v", calls)
	}
	f.put("refs/tags/a", []byte(snapshotTestID(2)))
	f.mu.Lock()
	delete(f.objects, "refs/tags/b")
	f.mu.Unlock()
	logs = load()
	if len(logs) != 1 || logs[0].ID != snapshotTestID(2) || logs[0].Memo != "second" {
		t.Fatalf("tag change missing: %+v", logs)
	}
	if calls := f.takeRequests(); len(calls) != 4 {
		t.Fatalf("changed tag requests: %v", calls)
	}
}

func TestCloudSnapshotCachePaginatedListingsAndSourceIsolation(t *testing.T) {
	f, repo := newSnapshotS3Fixture(t)
	f.listLimit = 1
	f.index(1, "one")
	f.index(2, "two")
	f.manifest(1, 2)
	cache := &CloudSnapshotCache{}
	loadSnapshotPage(t, repo, cache, 1)
	if calls := f.takeRequests(); len(calls) != 5 {
		t.Fatalf("metadata listing was not fully paginated: %v", calls)
	}
	loadSnapshotPage(t, repo, cache, 1)
	if calls := f.takeRequests(); len(calls) != 3 {
		t.Fatalf("warm paginated requests: %v", calls)
	}
	repo.cloud.GetConf().S3.AccessKey = "other-access"
	loadSnapshotPage(t, repo, cache, 1)
	if calls := f.takeRequests(); len(calls) != 5 || calls[0].conditional != "" {
		t.Fatalf("credentials shared cache: %v", calls)
	}
	wrong := *repo
	store, err := NewStore(t.TempDir(), []byte("11234567890123456789012345678901"))
	if err != nil {
		t.Fatal(err)
	}
	wrong.store = store
	load, err := wrong.NewCloudRepoLogLoader(cache)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err = load(1); err == nil {
		t.Fatal("cached index bypassed key validation")
	}
}

func TestCloudSnapshotCacheConcurrentQueries(t *testing.T) {
	f, repo := newSnapshotS3Fixture(t)
	f.index(1, "one")
	f.manifest(1)
	f.gateKey = "indexes-v2.json"
	f.entered = make(chan struct{})
	f.release = make(chan struct{})
	entered := f.entered
	cache := &CloudSnapshotCache{}
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for range 16 {
		wg.Go(func() {
			load, err := repo.NewCloudRepoLogLoader(cache)
			if err == nil {
				_, _, _, err = load(1)
			}
			errs <- err
		})
	}
	<-entered
	time.Sleep(50 * time.Millisecond)
	close(f.release)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if calls := f.takeRequests(); len(calls) != 3 {
		t.Fatalf("concurrent reads were not coalesced: %v", calls)
	}
}

func TestCloudSnapshotCacheRejectsInvalidDataAndObservesErrors(t *testing.T) {
	f, repo := newSnapshotS3Fixture(t)
	f.index(1, "valid")
	f.manifest(1)
	cache := &CloudSnapshotCache{}
	var observed []error
	repo.onCloudIndexList = func(err error) { observed = append(observed, err) }
	loadSnapshotPage(t, repo, cache, 1)
	for _, body := range []string{`null`, `{"indexes":[null]}`, `{"indexes":[{"id":"../bad"}]}`} {
		f.put("indexes-v2.json", f.encoder.EncodeAll([]byte(body), nil))
		if _, err := repo.NewCloudRepoLogLoader(cache); err == nil {
			t.Fatalf("invalid manifest accepted: %s", body)
		}
	}
	f.manifest(1)
	f.put("indexes/"+snapshotTestID(1), f.encoder.EncodeAll([]byte(`{"id":"wrong"}`), nil))
	load, err := repo.NewCloudRepoLogLoader(cache)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err = load(1); err == nil {
		t.Fatal("mismatched index accepted")
	}
	if len(observed) == 0 || observed[len(observed)-1] == nil {
		t.Fatal("index error was not observed")
	}
	f.index(1, "recovered")
	loadSnapshotPage(t, repo, cache, 1)
	if observed[len(observed)-1] != nil {
		t.Fatal("successful revalidation did not clear warning")
	}
	f.put("refs/tags/a", []byte("../bad"))
	if _, err = repo.GetCloudRepoTagLogsCached(cache, nil); err == nil {
		t.Fatal("invalid tag reference accepted")
	}
	f.mu.Lock()
	delete(f.objects, "indexes-v2.json")
	f.mu.Unlock()
	logs, pages, total := loadSnapshotPage(t, repo, cache, 1)
	if len(logs) != 0 || pages != 0 || total != 0 {
		t.Fatal("deleted manifest returned cached snapshots")
	}
	f.manifest()
	logs, _, _ = loadSnapshotPage(t, repo, cache, 1)
	if logs == nil || len(logs) != 0 {
		t.Fatal("empty manifest response changed")
	}
}

func TestCloudSnapshotCacheDoesNotCacheRacingDownloadAsListedVersion(t *testing.T) {
	f, repo := newSnapshotS3Fixture(t)
	f.index(1, "before")
	f.manifest(1)
	cache := &CloudSnapshotCache{}
	// 在取得列举结果后覆盖索引，下载响应的 ETag 与列举版本不同。
	load, err := repo.NewCloudRepoLogLoader(cache)
	if err != nil {
		t.Fatal(err)
	}
	f.index(1, "after")
	logs, _, _, err := load(1)
	if err != nil || len(logs) != 1 || logs[0].Memo != "after" {
		t.Fatalf("racing download: %+v %v", logs, err)
	}
	f.takeRequests()
	if _, _, _, err = load(1); err != nil {
		t.Fatal(err)
	}
	if calls := f.takeRequests(); len(calls) != 1 {
		t.Fatalf("download was cached under the stale listing version: %v", calls)
	}
}

type snapshotFallbackCloud struct {
	*cloud.BaseCloud
	calls int
}

func (fallback *snapshotFallbackCloud) GetIndexes(page int) ([]*entity.Index, int, int, error) {
	fallback.calls++
	return []*entity.Index{{ID: "legacy", Memo: fmt.Sprint(page)}}, 2, 64, nil
}

func (fallback *snapshotFallbackCloud) GetTags() ([]*cloud.Ref, error) {
	fallback.calls++
	return []*cloud.Ref{}, nil
}

func TestCloudSnapshotCacheKeepsOtherProvidersUnchanged(t *testing.T) {
	fallback := &snapshotFallbackCloud{BaseCloud: &cloud.BaseCloud{}}
	repo := &Repo{cloud: fallback}
	load, err := repo.NewCloudRepoLogLoader(&CloudSnapshotCache{})
	if err != nil {
		t.Fatal(err)
	}
	logs, pages, total, err := load(2)
	if err != nil || pages != 2 || total != 64 || len(logs) != 1 || logs[0].Memo != "2" || fallback.calls != 1 {
		t.Fatalf("provider fallback: %+v %v", logs, err)
	}
	if _, err = repo.GetCloudRepoTagLogsCached(&CloudSnapshotCache{}, nil); err != nil || fallback.calls != 2 {
		t.Fatalf("tag fallback: %v", err)
	}
}
