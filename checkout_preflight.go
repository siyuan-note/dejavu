package dejavu

import (
	"errors"
	"os"

	"github.com/siyuan-note/dejavu/entity"
)

// CheckoutPlan 提供同一批次的完整替换和删除集合；文件元数据为副本，不能用来修改实际计划。
type CheckoutPlan struct {
	Upserts []*entity.File
	Removes []*entity.File
	read    func(string) ([]byte, error)
}

// ReadFile 仅在预检回调内读取 Upserts 中指定路径的目标内容，不读取工作空间中的旧文件。
// 内容来自仓库的已认证分块；缺失分块按现有下载策略补齐，未知路径返回 os.ErrNotExist。
func (plan *CheckoutPlan) ReadFile(path string) ([]byte, error) {
	if plan.read == nil {
		return nil, errors.New("checkout preflight has ended")
	}
	return plan.read(path)
}

func (repo *Repo) preflightCheckout(upserts, removes []*entity.File, context map[string]interface{}) error {
	if err := repo.checkoutCanceled(); err != nil {
		return err
	}
	if repo.checkoutHooks.Preflight == nil || len(upserts)+len(removes) == 0 {
		return nil
	}
	if err := repo.prepareCheckout(); err != nil {
		return err
	}
	clone := func(files []*entity.File) []*entity.File {
		ret := make([]*entity.File, 0, len(files))
		for _, file := range files {
			copyFile := *file
			copyFile.Chunks = append([]string(nil), file.Chunks...)
			ret = append(ret, &copyFile)
		}
		return ret
	}
	plan := &CheckoutPlan{Upserts: clone(upserts), Removes: clone(removes)}
	byPath := filesByPath(upserts)
	plan.read = func(path string) ([]byte, error) {
		if err := repo.checkoutCanceled(); err != nil {
			return nil, err
		}
		file := byPath[path]
		if file == nil {
			return nil, os.ErrNotExist
		}
		if err := repo.ensureFileChunks(file, context); err != nil {
			return nil, err
		}
		return repo.openFile(file)
	}
	defer func() { plan.read = nil }()
	if err := repo.checkoutHooks.Preflight(plan); err != nil {
		return err
	}
	return repo.checkoutCanceled()
}
