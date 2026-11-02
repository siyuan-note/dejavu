package dejavu

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/88250/gulu"
)

var ErrSyncSourceChanged = errors.New("sync source changed; choose a one-way sync")
var ErrSyncSourcePreviewChanged = errors.New("sync preview changed; review the source again")

const (
	CtxExpectedSyncLocal = "expectedSyncLocal"
	CtxExpectedSyncCloud = "expectedSyncCloud"
)

type syncSourceBinding struct {
	Version  int    `json:"version"`
	Source   string `json:"source"`
	Baseline string `json:"baseline"`
}

// ConfigureSyncSource 绑定调用方提供的来源摘要，不包含密钥或连接选项。
// 旧仓库缺少记录时由调用方显式登记；未知版本和损坏记录必须保留并报错。
func (repo *Repo) ConfigureSyncSource(source string) error {
	lock.Lock()
	defer lock.Unlock()
	if source == "" {
		return errors.New("empty sync source")
	}
	if _, err := repo.readSyncSource(); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	repo.syncSource = source
	return nil
}

func (repo *Repo) readSyncSource() (*syncSourceBinding, error) {
	data, err := os.ReadFile(filepath.Join(repo.Path, "refs", "latest-sync-source"))
	if err != nil {
		return nil, err
	}
	binding := &syncSourceBinding{}
	if err = json.Unmarshal(data, binding); err != nil {
		return nil, fmt.Errorf("invalid sync source binding: %w", err)
	}
	if binding.Version != 1 || binding.Source == "" || !validSyncBaseline(binding.Baseline) {
		return nil, errors.New("unsupported or incomplete sync source binding")
	}
	return binding, nil
}

func (repo *Repo) writeSyncSource() error {
	if repo.syncSource == "" {
		return nil
	}
	baseline, err := os.ReadFile(filepath.Join(repo.Path, "refs", "latest-sync"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !validSyncBaseline(string(baseline)) {
		return errors.New("invalid sync baseline")
	}
	data, err := json.Marshal(syncSourceBinding{Version: 1, Source: repo.syncSource, Baseline: string(baseline)})
	if err != nil {
		return err
	}
	return gulu.File.WriteFileSafer(filepath.Join(repo.Path, "refs", "latest-sync-source"), data, 0600)
}

func validSyncBaseline(id string) bool {
	if len(id) != 40 {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil
}

// BindLegacySyncSource 仅为缺少记录的同步基点登记已知来源，不覆盖现有身份。
func (repo *Repo) BindLegacySyncSource() error {
	lock.Lock()
	defer lock.Unlock()
	if _, err := repo.readSyncSource(); errors.Is(err, os.ErrNotExist) {
		return repo.writeSyncSource()
	} else {
		return err
	}
}

func (repo *Repo) syncSourceChanged() (bool, error) {
	if repo.syncSource == "" {
		return false, nil
	}
	binding, err := repo.readSyncSource()
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	baseline, err := os.ReadFile(filepath.Join(repo.Path, "refs", "latest-sync"))
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return binding.Source != repo.syncSource || binding.Baseline != string(baseline), nil
}

// SyncSourceChanged 同时检查来源和基点，写入中断造成的失配也需要显式选择方向。
func (repo *Repo) SyncSourceChanged() (bool, error) {
	lock.Lock()
	defer lock.Unlock()
	return repo.syncSourceChanged()
}

func (repo *Repo) checkSyncSource(cloudID string) error {
	changed, err := repo.syncSourceChanged()
	if err != nil {
		return err
	}
	if changed && cloudID != "" {
		return ErrSyncSourceChanged
	}
	return nil
}

// finishSyncSource 在操作成功后记录二元组；基点先写、来源后写的中断会在下次检查中被拒绝。
func (repo *Repo) finishSyncSource(err *error) {
	if *err == nil {
		*err = repo.writeSyncSource()
	}
}

func checkSyncSourcePreview(context map[string]interface{}, localID, cloudID string) error {
	if expected, ok := context[CtxExpectedSyncLocal].(string); ok && expected != localID {
		return ErrSyncSourcePreviewChanged
	}
	if expected, ok := context[CtxExpectedSyncCloud].(string); ok && expected != cloudID {
		return ErrSyncSourcePreviewChanged
	}
	return nil
}
