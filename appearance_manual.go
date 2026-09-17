package dejavu

import (
	"time"

	"github.com/siyuan-note/dejavu/entity"
)

func (repo *Repo) appearanceCloudFiles(index *entity.Index, traffic *TrafficStat,
	context map[string]interface{}) ([]*entity.File, error) {
	missing, err := repo.localNotFoundFiles(index.Files)
	if err != nil {
		return nil, err
	}
	stat, _, err := repo.downloadCloudFilesPut(missing, context)
	if err != nil {
		return nil, err
	}
	traffic.DownloadBytes += stat.CloudBytes
	traffic.DownloadFileCount += len(missing) - stat.PeerCount
	traffic.APIGet += len(missing) - stat.PeerCount
	traffic.PeerDownloadBytes += stat.PeerBytes
	traffic.PeerDownloadFileCount += stat.PeerCount
	traffic.PeerFallbackCount += stat.PeerFallbackCount
	return repo.getFiles(index.Files)
}

func (repo *Repo) syncAppearanceUpload(latest, cloudLatest *entity.Index, latestFiles []*entity.File,
	traffic *TrafficStat, context map[string]interface{}) error {
	cloudFiles, err := repo.appearanceCloudFiles(cloudLatest, traffic, context)
	if err != nil {
		return err
	}
	if err = repo.prepareAppearanceSyncIgnore(latest, cloudFiles, context, "upload"); err != nil {
		return err
	}
	appearanceStat, err := repo.validateRemoteAppearanceFormat(cloudLatest, cloudFiles, context)
	addAppearanceTraffic(traffic, appearanceStat)
	if err != nil {
		return err
	}
	baseFiles, err := repo.getFiles(repo.latestSync().Files)
	if err != nil {
		return err
	}
	merge := &MergeResult{Time: time.Now()}
	effective, _, err := repo.mergeAppearanceEventFiles(baseFiles, latestFiles, cloudFiles, merge, traffic, context, "upload")
	if err != nil {
		return err
	}
	local := filesByPath(latestFiles)
	for _, file := range effective {
		key, _ := appearanceArchiveKey(file.Path)
		if key == "" && file.Path != appearanceFormatPath {
			continue
		}
		if previous := local[file.Path]; previous == nil || !equalFileContent(previous, file) {
			merge.Upserts = append(merge.Upserts, file)
		}
	}
	if err = repo.ensureAppearanceJournal(append(append([]*entity.File{}, latestFiles...), effective...)); err != nil {
		return err
	}
	if repo.assetDownloads == nil {
		if err = repo.uploadCloud(context, latest, cloudLatest, repo.getChunks(cloudFiles), traffic); err != nil {
			return err
		}
		if err = repo.updateCloudIndexes(latest, traffic, context); err != nil {
			return err
		}
		return repo.UpdateLatestSync(latest)
	}
	return repo.finishAssetSync(merge, true, true, latest, cloudLatest, repo.getChunks(cloudFiles), traffic, context, nil)
}
