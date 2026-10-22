package dejavu

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/siyuan-note/dejavu/entity"
)

// CheckoutProgress 描述当前文件的恢复进度，Written 只计算本轮已经写入临时文件的字节。
type CheckoutProgress struct {
	Path          string
	Written, Size int64
	Count, Total  int
}

// CheckoutHooks 在仓库操作前设置，不得在回调中重入仓库。回调只报告 DataPath 中的文件。
// 取消保留已完成的文件，不更新成功同步点，当前临时文件由退出清理。
type CheckoutHooks struct {
	Context context.Context
	// Apply 在改写数据目录前准备调用方；空差异和纯上传不会调用。
	Apply func() error
	// Applied 在本批落盘完成后恢复调用方索引，再进入后续网络阶段。
	Applied func()
	// Preflight 在本批实际落盘文件的替换及删除前检查目标内容；返回错误时不应用该批变更。
	// 回调可通过 CheckoutPlan.ReadFile 读取待写入的已认证内容，不得重入仓库或保留计划供异步使用。
	Preflight func(*CheckoutPlan) error
	// Before 在单个文件替换前检查磁盘空间等条件，不能用于需要整批拒绝的兼容检查。
	Before    func([]*entity.File) error
	Progress  func(CheckoutProgress)
	Committed func(*entity.File)
	Removed   func(*entity.File)
	Cleaned   func(int64)
}

func (repo *Repo) completeCheckout() {
	if repo.checkoutHooks.Applied != nil {
		repo.checkoutHooks.Applied()
	}
}

func (repo *Repo) prepareCheckout() error {
	if err := repo.checkoutCanceled(); err != nil {
		return err
	}
	if repo.checkoutHooks.Apply != nil {
		return repo.checkoutHooks.Apply()
	}
	return nil
}

func (repo *Repo) SetCheckoutHooks(hooks CheckoutHooks) {
	repo.checkoutHooks = hooks
	if backend, ok := repo.cloud.(interface{ SetContext(context.Context) }); ok {
		backend.SetContext(hooks.Context)
	}
}

func (repo *Repo) checkoutCanceled() error {
	if repo.checkoutHooks.Context != nil {
		return repo.checkoutHooks.Context.Err()
	}
	return nil
}

// normalizeCheckoutError 保留已取消请求的错误身份，包括未包装 context 错误的云服务。
func (repo *Repo) normalizeCheckoutError(err *error) {
	if *err != nil {
		if canceled := repo.checkoutCanceled(); canceled != nil && !errors.Is(*err, canceled) {
			*err = errors.Join(*err, canceled)
		}
	}
}

func (repo *Repo) reportCheckout(file *entity.File, written int64, count, total int) {
	if repo.checkoutHooks.Progress != nil {
		repo.checkoutHooks.Progress(CheckoutProgress{file.Path, written, file.Size, count, total})
	}
}

var checkoutTempName = regexp.MustCompile(`^\.siyuan-checkout-[a-f0-9]{40}-[0-9]+\.tmp$`)
var legacyCheckoutSuffix = regexp.MustCompile(`^[a-zA-Z0-9]{7}\.tmp$`)

// CleanupCheckoutTemps 清理本仓库管理的恢复临时文件，不跟随符号链接；应在启动或同步前调用。
// 与仓库操作共用锁，不会删除正在写入的文件。不按通用 .tmp 后缀删除用户文件。
func (repo *Repo) CleanupCheckoutTemps() (released int64, err error) {
	lock.Lock()
	defer lock.Unlock()
	return repo.cleanupCheckoutTemps(nil)
}

func (repo *Repo) cleanupCheckoutTemps(files []*entity.File) (released int64, err error) {
	legacy := map[string]*entity.File{}
	for _, file := range files {
		legacy[filepath.Clean(repo.absPath(file.Path))] = file
	}
	err = filepath.WalkDir(repo.DataPath, func(name string, entry fs.DirEntry, walkErr error) error {
		if errors.Is(walkErr, os.ErrNotExist) {
			return nil
		}
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		owned := checkoutTempName.MatchString(entry.Name())
		if !owned && strings.HasSuffix(name, ".tmp") && len(entry.Name()) > 11 {
			target := name[:len(name)-11]
			if file := legacy[target]; file != nil && legacyCheckoutSuffix.MatchString(entry.Name()[len(entry.Name())-11:]) {
				owned = repo.isCheckoutPrefix(name, file)
			}
		}
		if !owned {
			return nil
		}
		info, statErr := entry.Info()
		if statErr != nil {
			return statErr
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		if removeErr := os.Remove(name); removeErr != nil {
			return removeErr
		}
		released += info.Size()
		return nil
	})
	return
}

// isCheckoutPrefix 只在旧临时文件内容匹配已认证分块时确认其归属，无法认证时保留。
func (repo *Repo) isCheckoutPrefix(name string, file *entity.File) bool {
	f, err := os.Open(name)
	if err != nil {
		return false
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.Size() > file.Size || info.Size() == 0 {
		return false
	}
	remaining := info.Size()
	for _, id := range file.Chunks {
		chunk, err := repo.store.GetChunk(id)
		if err != nil {
			return false
		}
		length := min(int64(len(chunk.Data)), remaining)
		data := make([]byte, length)
		if _, err = io.ReadFull(f, data); err != nil || !bytes.Equal(data, chunk.Data[:length]) {
			return false
		}
		remaining -= length
		if remaining == 0 {
			return true
		}
	}
	return false
}
