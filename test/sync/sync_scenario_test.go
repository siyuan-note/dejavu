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

package syncscenario_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	dejavu "github.com/siyuan-note/dejavu"
	"github.com/siyuan-note/dejavu/cloud"
	"github.com/siyuan-note/encryption"
)

const (
	testRepoPassword     = "pass"
	testRepoPasswordSalt = "salt"

	syncScenarioCaseDir  = "testdata/cases"
	syncScenarioCloudDir = "sync-scenarios"
)

type syncScenarioCase struct {
	Name    string              `json:"name"`
	Skip    string              `json:"skip"`
	Seed    map[string]string   `json:"seed"`
	SeedDir string              `json:"seedDir"`
	Clients []string            `json:"clients"`
	Steps   []*syncScenarioStep `json:"steps"`
	Final   syncScenarioFinal   `json:"final"`

	baseDir string
}

type syncScenarioStep struct {
	Op        string                   `json:"op"`
	Client    string                   `json:"client"`
	Path      string                   `json:"path"`
	Content   string                   `json:"content"`
	Source    string                   `json:"source"`
	SourceDir string                   `json:"sourceDir"`
	Memo      string                   `json:"memo"`
	Minutes   int                      `json:"minutes"`
	Want      *syncScenarioExpectation `json:"want"`
}

type syncScenarioExpectation struct {
	Upserts   int `json:"upserts"`
	Removes   int `json:"removes"`
	Conflicts int `json:"conflicts"`
}

type syncScenarioFinal map[string]syncScenarioClientState

type syncScenarioClientState struct {
	Files   map[string]string `json:"files"`
	Sources map[string]string `json:"sources"`
	Missing []string          `json:"missing"`
}

type syncScenarioEnv struct {
	t             *testing.T
	root          string
	cloudEndpoint string
	caseBaseDir   string
	aesKey        []byte
}

type syncScenarioClient struct {
	env         *syncScenarioEnv
	name        string
	dataPath    string
	repoPath    string
	historyPath string
	tempPath    string
	repo        *dejavu.Repo
}

func TestSyncScenariosFromJSON(t *testing.T) {
	cases := loadSyncScenarioCases(t)
	if len(cases) < 1 {
		t.Fatalf("no sync scenario cases found in [%s]", syncScenarioCaseDir)
	}

	for _, testCase := range cases {
		testCase := testCase
		t.Run(testCase.Name, func(t *testing.T) {
			runSyncScenarioCase(t, testCase)
		})
	}
}

func loadSyncScenarioCases(t *testing.T) (ret []*syncScenarioCase) {
	t.Helper()

	entries, err := os.ReadDir(syncScenarioCaseDir)
	if err != nil {
		t.Fatalf("read sync scenario cases failed: %s", err)
	}

	for _, entry := range entries {
		var path string
		if entry.IsDir() {
			path = filepath.Join(syncScenarioCaseDir, entry.Name(), "config.json")
		} else if filepath.Ext(entry.Name()) == ".json" {
			path = filepath.Join(syncScenarioCaseDir, entry.Name())
		} else {
			continue
		}

		data, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatalf("read sync scenario case [%s] failed: %s", path, readErr)
		}

		data = bytes.TrimSpace(data)
		var cases []*syncScenarioCase
		if len(data) > 0 && data[0] == '[' {
			if err = json.Unmarshal(data, &cases); err != nil {
				t.Fatalf("unmarshal sync scenario case list [%s] failed: %s", path, err)
			}
		} else {
			testCase := &syncScenarioCase{}
			if err = json.Unmarshal(data, testCase); err != nil {
				t.Fatalf("unmarshal sync scenario case [%s] failed: %s", path, err)
			}
			cases = append(cases, testCase)
		}

		for _, testCase := range cases {
			if testCase.Name == "" {
				t.Fatalf("sync scenario case [%s] has empty name", path)
			}
			testCase.baseDir = filepath.Dir(path)
			ret = append(ret, testCase)
		}
	}
	return
}

func runSyncScenarioCase(t *testing.T, testCase *syncScenarioCase) {
	t.Helper()

	if testCase.Skip != "" {
		t.Skip(testCase.Skip)
	}

	env := newSyncScenarioEnv(t)
	env.caseBaseDir = testCase.baseDir
	base := env.seedSyncedClient("seed", testCase)
	clients := map[string]*syncScenarioClient{}
	for _, clientName := range testCase.Clients {
		if clientName == "" {
			t.Fatalf("case [%s] has empty client name", testCase.Name)
		}
		if _, exists := clients[clientName]; exists {
			t.Fatalf("case [%s] has duplicate client [%s]", testCase.Name, clientName)
		}
		clients[clientName] = env.cloneClient(base, clientName)
	}

	for i, step := range testCase.Steps {
		client := clients[step.Client]
		if client == nil {
			t.Fatalf("case [%s] step [%d] references unknown client [%s]", testCase.Name, i+1, step.Client)
		}
		runSyncScenarioStep(t, client, i+1, step)
	}

	assertSyncScenarioFinal(t, testCase.Name, clients, testCase.Final)
}

func runSyncScenarioStep(t *testing.T, client *syncScenarioClient, stepNum int, step *syncScenarioStep) {
	t.Helper()

	switch step.Op {
	case "write":
		content := step.Content
		if step.Source != "" {
			data, err := os.ReadFile(syncScenarioFixturePath(client.env.t, client.env.caseBaseDir, step.Source))
			if err != nil {
				client.env.t.Fatalf("[%s] read step source [%s] failed: %s", client.name, step.Source, err)
			}
			content = string(data)
		}
		client.writeFile(step.Path, content, syncScenarioBaseTime().Add(time.Duration(step.Minutes)*time.Minute))
	case "apply_dir":
		if step.SourceDir == "" {
			t.Fatalf("[%s] apply_dir step [%d] has empty sourceDir", client.name, stepNum)
		}
		sourceDir := syncScenarioFixturePath(client.env.t, client.env.caseBaseDir, step.SourceDir)
		syncScenarioCopyDirInto(client.env.t, sourceDir, client.dataPath)
		syncScenarioTouchDir(client.env.t, client.dataPath, syncScenarioBaseTime().Add(time.Duration(step.Minutes)*time.Minute))
	case "remove":
		client.removeFile(step.Path)
	case "index":
		memo := step.Memo
		if memo == "" {
			memo = "sync scenario index"
		}
		client.index(memo)
	case "sync":
		result := client.sync()
		if step.Want != nil {
			client.assertMergeResult(result, *step.Want)
		}
	case "sync_download":
		result := client.syncDownload()
		if step.Want != nil {
			client.assertMergeResult(result, *step.Want)
		}
	case "assert":
		client.assertFile(step.Path, step.Content)
	case "assert_missing":
		client.assertMissing(step.Path)
	default:
		t.Fatalf("[%s] unknown sync scenario step op [%s] at step [%d]", client.name, step.Op, stepNum)
	}
}

func assertSyncScenarioFinal(t *testing.T, caseName string, clients map[string]*syncScenarioClient, final syncScenarioFinal) {
	t.Helper()

	for clientName, state := range final {
		client := clients[clientName]
		if client == nil {
			t.Fatalf("case [%s] final references unknown client [%s]", caseName, clientName)
		}
		for path, content := range state.Files {
			client.assertFile(path, content)
		}
		for path, source := range state.Sources {
			data, err := os.ReadFile(syncScenarioFixturePath(t, client.env.caseBaseDir, source))
			if err != nil {
				t.Fatalf("case [%s] read final source [%s] failed: %s", caseName, source, err)
			}
			client.assertFile(path, string(data))
		}
		for _, path := range state.Missing {
			client.assertMissing(path)
		}
	}
}

func newSyncScenarioEnv(t *testing.T) *syncScenarioEnv {
	t.Helper()

	aesKey, err := encryption.KDF(testRepoPassword, testRepoPasswordSalt)
	if err != nil {
		t.Fatalf("kdf failed: %s", err)
	}

	root := t.TempDir()
	cloudEndpoint := filepath.Join(root, "cloud")
	if err = os.MkdirAll(cloudEndpoint, 0755); err != nil {
		t.Fatalf("mkdir cloud endpoint failed: %s", err)
	}

	return &syncScenarioEnv{
		t:             t,
		root:          root,
		cloudEndpoint: cloudEndpoint,
		aesKey:        aesKey,
	}
}

func (env *syncScenarioEnv) seedSyncedClient(name string, testCase *syncScenarioCase) *syncScenarioClient {
	env.t.Helper()

	if len(testCase.Seed) < 1 && testCase.SeedDir == "" {
		env.t.Fatalf("sync scenario seed must contain at least one file or seedDir")
	}

	client := env.newClient(name)
	mtime := syncScenarioBaseTime()
	if testCase.SeedDir != "" {
		seedDir := syncScenarioFixturePath(env.t, testCase.baseDir, testCase.SeedDir)
		syncScenarioCopyDirInto(env.t, seedDir, client.dataPath)
		syncScenarioTouchDir(env.t, client.dataPath, mtime)
	}
	for relPath, content := range testCase.Seed {
		client.writeFile(relPath, content, mtime)
	}
	client.index("seed")
	client.syncNoConflict(0, 0)
	return client
}

func (env *syncScenarioEnv) newClient(name string) *syncScenarioClient {
	env.t.Helper()

	root := filepath.Join(env.root, "clients", name)
	client := &syncScenarioClient{
		env:         env,
		name:        name,
		dataPath:    filepath.Join(root, "data"),
		repoPath:    filepath.Join(root, "repo"),
		historyPath: filepath.Join(root, "history"),
		tempPath:    filepath.Join(root, "temp"),
	}
	client.repo = env.newRepo(client)
	return client
}

func (env *syncScenarioEnv) cloneClient(src *syncScenarioClient, name string) *syncScenarioClient {
	env.t.Helper()

	client := env.newClient(name)
	syncScenarioCopyDir(env.t, src.dataPath, client.dataPath)
	syncScenarioCopyDir(env.t, src.repoPath, client.repoPath)
	client.repo = env.newRepo(client)
	return client
}

func (env *syncScenarioEnv) newRepo(client *syncScenarioClient) *dejavu.Repo {
	env.t.Helper()

	c := cloud.NewLocal(&cloud.BaseCloud{Conf: &cloud.Conf{
		Dir:    syncScenarioCloudDir,
		UserID: "0",
		Local: &cloud.ConfLocal{
			Endpoint:       env.cloudEndpoint,
			ConcurrentReqs: 4,
		},
	}})

	repo, err := dejavu.NewRepo(client.dataPath, client.repoPath, client.historyPath, client.tempPath, client.name, client.name, runtime.GOOS, env.aesKey, nil, c)
	if err != nil {
		env.t.Fatalf("new repo [%s] failed: %s", client.name, err)
	}
	return repo
}

func (client *syncScenarioClient) writeFile(relPath, content string, modTime time.Time) {
	client.env.t.Helper()

	absPath := filepath.Join(client.dataPath, filepath.FromSlash(relPath))
	if err := os.MkdirAll(filepath.Dir(absPath), 0755); err != nil {
		client.env.t.Fatalf("[%s] mkdir file parent failed: %s", client.name, err)
	}
	if err := os.WriteFile(absPath, []byte(content), 0644); err != nil {
		client.env.t.Fatalf("[%s] write file failed: %s", client.name, err)
	}
	if err := os.Chtimes(absPath, modTime, modTime); err != nil {
		client.env.t.Fatalf("[%s] chtimes failed: %s", client.name, err)
	}
}

func (client *syncScenarioClient) removeFile(relPath string) {
	client.env.t.Helper()

	err := os.Remove(filepath.Join(client.dataPath, filepath.FromSlash(relPath)))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		client.env.t.Fatalf("[%s] remove file failed: %s", client.name, err)
	}
}

func (client *syncScenarioClient) index(memo string) {
	client.env.t.Helper()

	if _, err := client.repo.Index(memo, true, map[string]interface{}{}); err != nil {
		client.env.t.Fatalf("[%s] index failed: %s", client.name, err)
	}
}

func (client *syncScenarioClient) sync() *dejavu.MergeResult {
	client.env.t.Helper()

	mergeResult, _, err := client.repo.Sync(map[string]interface{}{})
	if err != nil {
		client.env.t.Fatalf("[%s] sync failed: %s", client.name, err)
	}
	return mergeResult
}

func (client *syncScenarioClient) syncDownload() *dejavu.MergeResult {
	client.env.t.Helper()

	mergeResult, _, err := client.repo.SyncDownload(map[string]interface{}{})
	if err != nil {
		client.env.t.Fatalf("[%s] sync download failed: %s", client.name, err)
	}
	return mergeResult
}

func (client *syncScenarioClient) syncNoConflict(wantUpserts, wantRemoves int) *dejavu.MergeResult {
	client.env.t.Helper()

	mergeResult := client.sync()
	client.assertMergeResult(mergeResult, syncScenarioExpectation{Upserts: wantUpserts, Removes: wantRemoves})
	return mergeResult
}

func (client *syncScenarioClient) assertMergeResult(mergeResult *dejavu.MergeResult, want syncScenarioExpectation) {
	client.env.t.Helper()

	if len(mergeResult.Upserts) != want.Upserts || len(mergeResult.Removes) != want.Removes || len(mergeResult.Conflicts) != want.Conflicts {
		client.env.t.Fatalf("[%s] expected upserts=%d removes=%d conflicts=%d, got upserts=%d removes=%d conflicts=%d",
			client.name, want.Upserts, want.Removes, want.Conflicts,
			len(mergeResult.Upserts), len(mergeResult.Removes), len(mergeResult.Conflicts))
	}
}

func (client *syncScenarioClient) assertFile(relPath, want string) {
	client.env.t.Helper()

	data, err := os.ReadFile(filepath.Join(client.dataPath, filepath.FromSlash(relPath)))
	if err != nil {
		client.env.t.Fatalf("[%s] read file failed: %s", client.name, err)
	}
	if string(data) != want {
		client.env.t.Fatalf("[%s] file [%s] mismatch: got %q, want %q", client.name, relPath, data, want)
	}
}

func (client *syncScenarioClient) assertMissing(relPath string) {
	client.env.t.Helper()

	_, err := os.Stat(filepath.Join(client.dataPath, filepath.FromSlash(relPath)))
	if err == nil {
		client.env.t.Fatalf("[%s] expected [%s] to be missing", client.name, relPath)
	}
	if !errors.Is(err, os.ErrNotExist) {
		client.env.t.Fatalf("[%s] stat [%s] failed: %s", client.name, relPath, err)
	}
}

func syncScenarioBaseTime() time.Time {
	return time.Unix(1700000000, 0)
}

func syncScenarioFixturePath(t *testing.T, baseDir, relPath string) string {
	t.Helper()

	if relPath == "" || !filepath.IsLocal(relPath) {
		t.Fatalf("invalid fixture path [%s]", relPath)
	}
	return filepath.Join(baseDir, filepath.FromSlash(relPath))
}

func syncScenarioCopyDirInto(t *testing.T, src, dst string) {
	t.Helper()

	if err := filepath.WalkDir(src, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, relPath)

		info, err := d.Info()
		if err != nil {
			return err
		}
		if d.IsDir() {
			return os.MkdirAll(target, info.Mode().Perm())
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		return syncScenarioCopyFile(target, path, info.Mode().Perm(), info.ModTime())
	}); err != nil {
		t.Fatalf("copy dir into [%s -> %s] failed: %s", src, dst, err)
	}
}

func syncScenarioTouchDir(t *testing.T, dir string, modTime time.Time) {
	t.Helper()

	if err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		return os.Chtimes(path, modTime, modTime)
	}); err != nil {
		t.Fatalf("touch dir [%s] failed: %s", dir, err)
	}
}

func syncScenarioCopyDir(t *testing.T, src, dst string) {
	t.Helper()

	if err := os.RemoveAll(dst); err != nil {
		t.Fatalf("remove dst [%s] failed: %s", dst, err)
	}
	if err := filepath.WalkDir(src, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, relPath)

		info, err := d.Info()
		if err != nil {
			return err
		}
		if d.IsDir() {
			return os.MkdirAll(target, info.Mode().Perm())
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		return syncScenarioCopyFile(target, path, info.Mode().Perm(), info.ModTime())
	}); err != nil {
		t.Fatalf("copy dir [%s -> %s] failed: %s", src, dst, err)
	}
}

func syncScenarioCopyFile(dst, src string, perm fs.FileMode, modTime time.Time) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}

	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	if _, err = io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	if err = out.Close(); err != nil {
		return err
	}
	return os.Chtimes(dst, modTime, modTime)
}
