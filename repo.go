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

package dejavu

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/88250/gulu"
	"github.com/panjf2000/ants/v2"
	"github.com/restic/chunker"
	ignore "github.com/sabhiram/go-gitignore"
	"github.com/siyuan-note/dejavu/cloud"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/dejavu/util"
	"github.com/siyuan-note/eventbus"
	"github.com/siyuan-note/filelock"
	"github.com/siyuan-note/logging"
)

// Repo 描述了逮虾户数据仓库。
type Repo struct {
	DataPath    string   // 数据文件夹的绝对路径，如：F:\\SiYuan\\data\\
	Path        string   // 仓库的绝对路径，如：F:\\SiYuan\\repo\\
	HistoryPath string   // 数据历史文件夹的绝对路径，如：F:\\SiYuan\\history\\
	TempPath    string   // 临时文件夹的绝对路径，如：F:\\SiYuan\\temp\\
	IgnoreLines []string // 忽略配置文件内容行，是用 .gitignore 语法

	store    *Store      // 仓库的存储
	chunkPol chunker.Pol // 文件分块多项式值
	cloud    cloud.Cloud // 云端存储服务
}

// NewRepo 创建一个新的仓库。
func NewRepo(dataPath, repoPath, historyPath, tempPath string, aesKey []byte, ignoreLines []string, cloud cloud.Cloud) (ret *Repo, err error) {
	if nil != cloud {
		cloud.GetConf().RepoPath = repoPath
	}
	ret = &Repo{
		DataPath:    filepath.Clean(dataPath),
		Path:        filepath.Clean(repoPath),
		HistoryPath: filepath.Clean(historyPath),
		TempPath:    filepath.Clean(tempPath),
		cloud:       cloud,
		chunkPol:    chunker.Pol(0x3DA3358B4DC173), // 固定分块多项式值
	}
	if !strings.HasSuffix(ret.DataPath, string(os.PathSeparator)) {
		ret.DataPath += string(os.PathSeparator)
	}
	if !strings.HasSuffix(ret.Path, string(os.PathSeparator)) {
		ret.Path += string(os.PathSeparator)
	}
	if !strings.HasSuffix(ret.HistoryPath, string(os.PathSeparator)) {
		ret.HistoryPath += string(os.PathSeparator)
	}
	ignoreLines = gulu.Str.RemoveDuplicatedElem(ignoreLines)
	ret.IgnoreLines = ignoreLines
	ret.store, err = NewStore(ret.Path, aesKey)
	return
}

var ErrRepoFatalErr = errors.New("repo fatal error")

var lock = sync.Mutex{} // 仓库锁， Checkout、Index 和 Sync 等不能同时执行

// GetIndex 从仓库根据 id 获取索引。
func (repo *Repo) GetIndex(id string) (index *entity.Index, err error) {
	lock.Lock()
	defer lock.Unlock()
	return repo.store.GetIndex(id)
}

// PutIndex 将索引 index 写入仓库。
func (repo *Repo) PutIndex(index *entity.Index) (err error) {
	lock.Lock()
	defer lock.Unlock()
	return repo.store.PutIndex(index)
}

var workspaceDataDirs = []string{"assets", "emojis", "snippets", "storage", "templates", "widgets"}
var removeEmptyDirExcludes = append(workspaceDataDirs, ".git")

// Checkout 将仓库中的数据迁出到 repo 数据文件夹下。context 参数用于发布事件时传递调用上下文。
func (repo *Repo) Checkout(id string, context map[string]interface{}) (upserts, removes []*entity.File, err error) {
	lock.Lock()
	defer lock.Unlock()

	index, err := repo.store.GetIndex(id)
	if nil != err {
		return
	}

	if err = os.MkdirAll(repo.DataPath, 0755); nil != err {
		return
	}
	var files []*entity.File
	ignoreMatcher := repo.ignoreMatcher()
	eventbus.Publish(eventbus.EvtCheckoutBeforeWalkData, context, repo.DataPath)
	err = filepath.Walk(repo.DataPath, func(path string, info os.FileInfo, err error) error {
		if nil != err {
			return io.EOF
		}
		if ignored, ignoreResult := repo.builtInIgnore(info, path); ignored || nil != ignoreResult {
			return ignoreResult
		}

		p := repo.relPath(path)
		if ignoreMatcher.MatchesPath(p) {
			return nil
		}

		files = append(files, entity.NewFile(p, info.Size(), info.ModTime().UnixMilli()))
		eventbus.Publish(eventbus.EvtCheckoutWalkData, context, p)
		return nil
	})
	if nil != err {
		return
	}

	defer gulu.File.RemoveEmptyDirs(repo.DataPath, removeEmptyDirExcludes...)

	latestFiles, err := repo.getFiles(index.Files)
	if nil != err {
		return
	}

	upserts, removes = repo.DiffUpsertRemove(latestFiles, files)
	if 1 > len(upserts) && 1 > len(removes) {
		return
	}

	waitGroup := &sync.WaitGroup{}
	var errs []error
	errLock := &sync.Mutex{}
	p, _ := ants.NewPoolWithFunc(2, func(arg interface{}) {
		defer waitGroup.Done()
		file := arg.(*entity.File)
		file, getErr := repo.store.GetFile(file.ID)
		if nil != getErr {
			errLock.Lock()
			errs = append(errs, getErr)
			errLock.Unlock()
			return
		}

		data, openErr := repo.openFile(file)
		if nil != openErr {
			errLock.Lock()
			errs = append(errs, getErr)
			errLock.Unlock()
			return
		}

		absPath := filepath.Join(repo.DataPath, file.Path)
		dir := filepath.Dir(absPath)

		if mkErr := os.MkdirAll(dir, 0755); nil != mkErr {
			errLock.Lock()
			errs = append(errs, mkErr)
			errLock.Unlock()
			return
		}

		if writeErr := filelock.WriteFile(absPath, data); nil != writeErr {
			errLock.Lock()
			errs = append(errs, writeErr)
			errLock.Unlock()
			return
		}

		updated := time.UnixMilli(file.Updated)
		if chtErr := os.Chtimes(absPath, updated, updated); nil != chtErr {
			logging.LogErrorf("change [%s] time failed: %s", absPath, chtErr)
			errLock.Lock()
			errs = append(errs, chtErr)
			errLock.Unlock()
			return
		}
		eventbus.Publish(eventbus.EvtCheckoutUpsertFile, context, file.Path)
	})

	eventbus.Publish(eventbus.EvtCheckoutUpsertFiles, context, upserts)
	for _, f := range upserts {
		waitGroup.Add(1)
		err = p.Invoke(f)
		if nil != err {
			return
		}
		if 0 < len(errs) {
			err = errs[0]
			return
		}
	}

	waitGroup.Wait()
	p.Release()

	if 0 < len(errs) {
		err = errs[0]
		return
	}

	eventbus.Publish(eventbus.EvtCheckoutRemoveFiles, context, removes)
	for _, f := range removes {
		absPath := repo.absPath(f.Path)
		if err = filelock.Remove(absPath); nil != err {
			return
		}
		eventbus.Publish(eventbus.EvtCheckoutRemoveFile, context, f.Path)
	}
	return
}

// Index 将 repo 数据文件夹中的文件索引到仓库中。context 参数用于发布事件时传递调用上下文。
func (repo *Repo) Index(memo string, context map[string]interface{}) (ret *entity.Index, err error) {
	lock.Lock()
	defer lock.Unlock()

	ret, err = repo.index(memo, context)
	return
}

// GetFiles 返回快照索引 index 中的文件列表。
func (repo *Repo) GetFiles(index *entity.Index) (ret []*entity.File, err error) {
	ret, err = repo.getFiles(index.Files)
	return
}

func (repo *Repo) GetFile(fileID string) (ret *entity.File, err error) {
	ret, err = repo.store.GetFile(fileID)
	return
}

func (repo *Repo) OpenFile(file *entity.File) (ret []byte, err error) {
	ret, err = repo.openFile(file)
	return
}

func (repo *Repo) OpenFileByID(fileID string) (ret []byte, err error) {
	f, err := repo.store.GetFile(fileID)
	if nil != err {
		return
	}

	ret, err = repo.openFile(f)
	return
}

func (repo *Repo) index(memo string, context map[string]interface{}) (ret *entity.Index, err error) {
	var files []*entity.File
	ignoreMatcher := repo.ignoreMatcher()
	eventbus.Publish(eventbus.EvtIndexBeforeWalkData, context, repo.DataPath)
	err = filepath.Walk(repo.DataPath, func(path string, info os.FileInfo, err error) error {
		if nil != err {
			return io.EOF
		}
		if ignored, ignoreResult := repo.builtInIgnore(info, path); ignored || nil != ignoreResult {
			return ignoreResult
		}

		p := repo.relPath(path)
		if ignoreMatcher.MatchesPath(p) {
			return nil
		}

		files = append(files, entity.NewFile(p, info.Size(), info.ModTime().UnixMilli()))
		eventbus.Publish(eventbus.EvtIndexWalkData, context, p)
		return nil
	})
	if nil != err {
		return
	}

	latest, err := repo.Latest()
	init := false
	if nil != err {
		if ErrNotFoundIndex != err {
			return
		}

		// 如果没有索引，则创建第一个索引
		latest = &entity.Index{ID: util.RandHash(), Memo: memo, Created: time.Now().UnixMilli()}
		init = true
	}
	var upserts, removes, latestFiles []*entity.File
	if !init {
		eventbus.Publish(eventbus.EvtIndexBeforeGetLatestFiles, context, latest.Files)
		for _, f := range latest.Files {
			eventbus.Publish(eventbus.EvtIndexGetLatestFile, context, f)
			var file *entity.File
			file, err = repo.store.GetFile(f)
			if nil != err {
				logging.LogErrorf("get file [%s] failed: %s", f, err)
				err = ErrNotFoundObject
				return
			}
			latestFiles = append(latestFiles, file)
		}
	}
	upserts, removes = repo.DiffUpsertRemove(files, latestFiles)
	if 1 > len(upserts) && 1 > len(removes) {
		ret = latest
		return
	}

	if init {
		ret = latest
	} else {
		ret = &entity.Index{
			ID:      util.RandHash(),
			Parent:  latest.ID,
			Memo:    memo,
			Created: time.Now().UnixMilli(),
		}
	}

	eventbus.Publish(eventbus.EvtIndexUpsertFiles, context, upserts)
	for _, file := range upserts {
		absPath := repo.absPath(file.Path)
		chunks, hashes, chunkErr := repo.fileChunks(absPath)
		if nil != chunkErr {
			err = chunkErr
			return
		}
		file.Chunks = hashes

		for _, chunk := range chunks {
			err = repo.store.PutChunk(chunk)
			if nil != err {
				return
			}
		}

		eventbus.Publish(eventbus.EvtIndexUpsertFile, context, file.Path)
		err = repo.store.PutFile(file)
		if nil != err {
			return
		}
	}

	for _, file := range files {
		ret.Files = append(ret.Files, file.ID)
		ret.Size += file.Size
	}
	ret.Count = len(ret.Files)

	err = repo.store.PutIndex(ret)
	if nil != err {
		return
	}

	err = repo.UpdateLatest(ret.ID)
	return
}

func (repo *Repo) builtInIgnore(info os.FileInfo, absPath string) (ignored bool, err error) {
	name := info.Name()
	if info.IsDir() {
		if strings.HasPrefix(name, ".") {
			if ".siyuan" == name {
				return true, nil
			}
			return true, filepath.SkipDir
		}
		return true, nil
	} else {
		if strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".tmp") {
			return true, nil
		}

		slashAbsPath := filepath.ToSlash(absPath)
		if strings.HasSuffix(slashAbsPath, "data/storage/local.json") {
			// localStorage 不再支持同步 https://github.com/siyuan-note/siyuan/issues/6964
			return true, nil
		}
		if strings.HasSuffix(slashAbsPath, "data/storage/recent-doc.json") {
			// 数据同步忽略最近文档存储 https://github.com/siyuan-note/siyuan/issues/7246
			return true, nil
		}
	}

	if gulu.File.IsHidden(absPath) {
		return true, nil
	}

	if !info.Mode().IsRegular() {
		return true, nil
	}
	return false, nil
}

func (repo *Repo) ignoreMatcher() *ignore.GitIgnore {
	return ignore.CompileIgnoreLines(repo.IgnoreLines...)
}

func (repo *Repo) absPath(relPath string) string {
	return filepath.Join(repo.DataPath, relPath)
}

func (repo *Repo) relPath(absPath string) string {
	absPath = filepath.Clean(absPath)
	return "/" + filepath.ToSlash(strings.TrimPrefix(absPath, repo.DataPath))
}

func (repo *Repo) fileChunks(absPath string) (chunks []*entity.Chunk, chunkHashes []string, err error) {
	info, statErr := os.Stat(absPath)
	if nil != statErr {
		logging.LogErrorf("stat file [%s] failed: %s", absPath, statErr)
		err = statErr
		return
	}

	if chunker.MinSize > info.Size() {
		data, readErr := filelock.ReadFile(absPath)
		if nil != readErr {
			logging.LogErrorf("read file [%s] failed: %s", absPath, readErr)
			err = readErr
			return
		}
		chnkHash := util.Hash(data)
		chunks = append(chunks, &entity.Chunk{ID: chnkHash, Data: data})
		chunkHashes = append(chunkHashes, chnkHash)
		return
	}

	reader, err := os.OpenFile(absPath, os.O_RDWR, 0644)
	if nil != err {
		logging.LogErrorf("open file [%s] failed: %s", absPath, err)
		return
	}

	chnkr := chunker.NewWithBoundaries(reader, repo.chunkPol, chunker.MinSize, chunker.MaxSize)
	for {
		buf := make([]byte, chunker.MaxSize)
		chnk, chnkErr := chnkr.Next(buf)
		if io.EOF == chnkErr {
			break
		}
		if nil != chnkErr {
			err = chnkErr
			break
		}

		chnkHash := util.Hash(chnk.Data)
		chunks = append(chunks, &entity.Chunk{ID: chnkHash, Data: chnk.Data})
		chunkHashes = append(chunkHashes, chnkHash)
	}

	if nil != err {
		logging.LogErrorf("chunk file [%s] failed: %s", absPath, err)
		if closeErr := reader.Sync(); nil != closeErr {
			logging.LogErrorf("sync file [%s] failed: %s", absPath, closeErr)
		}
		if closeErr := reader.Close(); nil != closeErr {
			logging.LogErrorf("close file [%s] failed: %s", absPath, closeErr)
		}
		return
	}

	if closeErr := reader.Sync(); nil != closeErr {
		logging.LogErrorf("sync file [%s] failed: %s", absPath, closeErr)
	}
	if closeErr := reader.Close(); nil != closeErr {
		logging.LogErrorf("close file [%s] failed: %s", absPath, closeErr)
	}
	return
}

func (repo *Repo) getFiles(fileIDs []string) (ret []*entity.File, err error) {
	for _, fileID := range fileIDs {
		file, getErr := repo.store.GetFile(fileID)
		if nil != getErr {
			err = getErr
			return
		}
		ret = append(ret, file)
	}
	return
}

func (repo *Repo) openFile(file *entity.File) (ret []byte, err error) {
	for _, c := range file.Chunks {
		var chunk *entity.Chunk
		chunk, err = repo.store.GetChunk(c)
		if nil != err {
			return
		}
		ret = append(ret, chunk.Data...)
	}
	return
}
