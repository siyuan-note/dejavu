package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/siyuan-note/dejavu"
	"github.com/siyuan-note/dejavu/cloud"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/logging"
)

const baseline = "464364dd8fadfdee29735024c30025cc65ed55aa"

type request struct {
	RecursiveRefs bool     `json:"recursiveRefs"`
	Data          string   `json:"data"`
	Repo          string   `json:"repo"`
	History       string   `json:"history"`
	Temp          string   `json:"temp"`
	Cloud         string   `json:"cloud"`
	CloudDir      string   `json:"cloudDir"`
	Device        string   `json:"device"`
	KeyHex        string   `json:"keyHex"`
	Ignore        []string `json:"ignore"`
	AssetState    string   `json:"assetState"`
	AssetScope    string   `json:"assetScope"`
	OnDemand      bool     `json:"onDemand"`
	Actions       []action `json:"actions"`
}

type action struct {
	Op          string   `json:"op"`
	ID          string   `json:"id"`
	Memo        string   `json:"memo"`
	Tag         string   `json:"tag"`
	Retention   []string `json:"retention"`
	CheckChunks bool     `json:"checkChunks"`
}

type result struct {
	Op    string `json:"op"`
	Value any    `json:"value,omitempty"`
	Error string `json:"error,omitempty"`
}

type response struct {
	Baseline string   `json:"baseline"`
	Provider string   `json:"provider,omitempty"`
	Results  []result `json:"results"`
	Error    string   `json:"error,omitempty"`
}

func main() {
	configPath := flag.String("config", "", "temporary JSON request file")
	flag.Parse()
	ret := response{Baseline: baseline}
	if err := execute(*configPath, &ret); err != nil {
		ret.Error = err.Error()
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(ret); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if ret.Error != "" {
		os.Exit(1)
	}
}

// temporaryPath 拒绝系统临时目录以外的目标，并核对已存在父目录的真实路径。
func temporaryPath(p string) error {
	if p == "" {
		return errors.New("missing temporary path")
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return err
	}
	tempRoot, err := filepath.EvalSymlinks(os.TempDir())
	if err != nil {
		return err
	}
	current := abs
	for {
		resolved, resolveErr := filepath.EvalSymlinks(current)
		if resolveErr == nil {
			rel, relErr := filepath.Rel(tempRoot, resolved)
			if relErr != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
				return fmt.Errorf("path is outside system temporary directory: %s", abs)
			}
			if abs == tempRoot {
				return errors.New("temporary root itself cannot be a target")
			}
			return nil
		}
		if !errors.Is(resolveErr, os.ErrNotExist) {
			return resolveErr
		}
		parent := filepath.Dir(current)
		if parent == current {
			return resolveErr
		}
		current = parent
	}
}

func execute(configPath string, ret *response) error {
	if err := temporaryPath(configPath); err != nil {
		return err
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		return err
	}
	var req request
	if err = json.Unmarshal(data, &req); err != nil {
		return err
	}
	for _, p := range []string{req.Data, req.Repo, req.History, req.Temp, req.Cloud} {
		if err = temporaryPath(p); err != nil {
			return err
		}
		if err = os.MkdirAll(p, 0755); err != nil {
			return err
		}
	}
	logging.SetLogPath(filepath.Join(req.Temp, "legacy-driver.log"))
	logging.SetLogToStdout(false)
	key := []byte("0123456789abcdef0123456789abcdef")
	if req.KeyHex != "" {
		if key, err = hex.DecodeString(req.KeyHex); err != nil {
			return err
		}
	}
	if req.CloudDir == "" {
		req.CloudDir = "main"
	}
	if filepath.Base(req.CloudDir) != req.CloudDir || strings.ContainsAny(req.CloudDir, "/\\:") || req.CloudDir == "." || req.CloudDir == ".." {
		return errors.New("cloudDir must be a single directory name")
	}
	if req.Device == "" {
		req.Device = "legacy-client"
	}
	var local cloud.Cloud = cloud.NewLocal(&cloud.BaseCloud{Conf: &cloud.Conf{
		Dir: req.CloudDir, RepoPath: req.Repo, AvailableSize: 1 << 40,
		Local: &cloud.ConfLocal{Endpoint: req.Cloud},
	}})
	ret.Provider = "original-local"
	if req.RecursiveRefs {
		local = &recursiveRefCloud{Local: local.(*cloud.Local)}
		ret.Provider = "local-with-recursive-ref-listing"
	}
	repo, err := dejavu.NewRepo(req.Data, req.Repo, req.History, req.Temp, req.Device, req.Device, runtime.GOOS, key, req.Ignore, local)
	if err != nil {
		return err
	}
	if req.AssetState != "" {
		if err = temporaryPath(req.AssetState); err != nil {
			return err
		}
		if err = repo.ConfigureAssetDownloads(req.OnDemand, req.AssetState, req.AssetScope); err != nil {
			return err
		}
	}
	ctx := map[string]interface{}{}
	lastID := ""
	resolveID := func(id string) (string, error) {
		if id == "last" && lastID != "" {
			return lastID, nil
		}
		if id == "" || id == "latest" {
			index, getErr := repo.Latest()
			if getErr != nil {
				return "", getErr
			}
			return index.ID, nil
		}
		return id, nil
	}
	for _, item := range req.Actions {
		entry := result{Op: item.Op}
		switch item.Op {
		case "index":
			var index *entity.Index
			index, err = repo.Index(item.Memo, item.CheckChunks, ctx)
			entry.Value = index
			if err == nil {
				lastID = index.ID
			}
		case "sync", "syncDownload":
			var merge *dejavu.MergeResult
			var stat *dejavu.TrafficStat
			if item.Op == "sync" {
				merge, stat, err = repo.Sync(ctx)
			} else {
				merge, stat, err = repo.SyncDownload(ctx)
			}
			entry.Value = map[string]any{"merge": merge, "stat": stat}
		case "syncUpload":
			entry.Value, err = repo.SyncUpload(ctx)
		case "latest":
			entry.Value, err = repo.Latest()
		case "cloudLatest":
			entry.Value, err = repo.GetCloudLatest(ctx)
		case "purge":
			entry.Value, err = repo.Purge(context.Background(), item.Retention...)
		case "purgeCloud":
			entry.Value, err = repo.PurgeCloud()
		case "removeTag":
			err = repo.RemoveTag(item.Tag)
		case "removeCloudTag":
			err = repo.RemoveCloudRepoTag(item.Tag)
		case "inspect", "checkout", "addTag", "uploadTag", "downloadTag", "downloadIndex":
			var id string
			id, err = resolveID(item.ID)
			if err != nil {
				break
			}
			switch item.Op {
			case "inspect":
				entry.Value, err = inspect(repo, id)
			case "checkout":
				var upserts, removes []*entity.File
				upserts, removes, err = repo.Checkout(id, ctx)
				entry.Value = map[string]any{"upserts": upserts, "removes": removes}
			case "addTag":
				err = repo.AddTag(id, item.Tag)
			case "uploadTag", "downloadTag", "downloadIndex":
				var files, chunks int
				var bytes int64
				if item.Op == "uploadTag" {
					files, chunks, bytes, err = repo.UploadTagIndex(item.Tag, id, ctx)
				} else if item.Op == "downloadTag" {
					files, chunks, bytes, err = repo.DownloadTagIndex(item.Tag, id, ctx)
				} else {
					files, chunks, bytes, err = repo.DownloadIndex(id, ctx)
				}
				entry.Value = map[string]any{"files": files, "chunks": chunks, "bytes": bytes}
			}
		default:
			err = fmt.Errorf("unknown action: %s", item.Op)
		}
		if err != nil {
			entry.Error = err.Error()
		}
		ret.Results = append(ret.Results, entry)
		if err != nil {
			return fmt.Errorf("%s failed: %w", item.Op, err)
		}
	}
	return nil
}

func inspect(repo *dejavu.Repo, id string) (any, error) {
	index, err := repo.GetIndex(id)
	if err != nil {
		return nil, err
	}
	files, err := repo.GetFiles(index)
	if err != nil {
		return nil, err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	ret := make([]map[string]any, 0, len(files))
	for _, file := range files {
		data, readErr := repo.OpenFile(file)
		if readErr != nil {
			return nil, readErr
		}
		digest := sha256.Sum256(data)
		ret = append(ret, map[string]any{"file": file, "sha256": hex.EncodeToString(digest[:])})
	}
	return map[string]any{"index": index, "files": ret}, nil
}

// recursiveRefCloud 仅为测试补齐本地后端的引用递归枚举契约，不改变固定旧版同步引擎。
type recursiveRefCloud struct {
	*cloud.Local
}

func (local *recursiveRefCloud) ListObjects(prefix string) (map[string]*entity.ObjectInfo, error) {
	if strings.TrimSuffix(prefix, "/") != "refs" {
		return local.Local.ListObjects(prefix)
	}
	root := filepath.Join(local.GetConf().Local.Endpoint, local.GetConf().Dir, "refs")
	ret := map[string]*entity.ObjectInfo{}
	err := filepath.WalkDir(root, func(p string, entry os.DirEntry, walkErr error) error {
		if errors.Is(walkErr, os.ErrNotExist) && p == root {
			return nil
		}
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		ret[rel] = &entity.ObjectInfo{Path: rel, Size: info.Size()}
		return nil
	})
	return ret, err
}
