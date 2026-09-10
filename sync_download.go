package dejavu

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/siyuan-note/dejavu/cloud"
	"github.com/siyuan-note/logging"
)

// downloadCloudIndexData 仅对索引解压和解析失败重下两次，不重试鉴权或密钥验证失败。
// validate 每次调用均应解析到新对象，避免失败尝试留下的字段污染后续结果。
func (repo *Repo) downloadCloudIndexData(key string, validate func([]byte) error) (downloadBytes int64, err error) {
	for attempt := 1; attempt <= 3; attempt++ {
		if attempt > 1 {
			time.Sleep(time.Duration(attempt-1) * 100 * time.Millisecond)
		}
		data, downloadErr := repo.cloud.DownloadObject(key)
		downloadBytes += int64(len(data))
		if nil != downloadErr {
			// 已读到坏对象后出现不存在响应，不能按首次创建处理，否则可能覆盖历史列表。
			if attempt > 1 && errors.Is(downloadErr, cloud.ErrCloudObjectNotFound) {
				return downloadBytes, fmt.Errorf("cloud index [%s] disappeared during retry: %w", key, err)
			}
			return downloadBytes, downloadErr
		}

		decoded, decodeErr := repo.store.compressDecoder.DecodeAll(data, nil)
		stage := "decompress"
		err = decodeErr
		if nil == err {
			stage = "parse"
			err = validate(decoded)
		}
		if nil == err {
			return
		}
		// 仅记录长度和摘要，不输出可能包含用户信息的对象正文。
		logging.LogWarnf("read cloud index [%s] failed [attempt=%d/3, stage=%s, bytes=%d, sha256=%x, decodedBytes=%d, decodedSHA256=%x]: %s",
			key, attempt, stage, len(data), sha256.Sum256(data), len(decoded), sha256.Sum256(decoded), err)
	}
	return downloadBytes, fmt.Errorf("read cloud index [%s] failed after 3 attempts: %w", key, err)
}
