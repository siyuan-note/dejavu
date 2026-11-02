package dejavu

import "github.com/siyuan-note/dejavu/entity"

type SyncSourcePreview struct {
	LocalID                                              string
	LocalFiles, UploadFiles, OverwriteFiles, RemoveFiles int
}

// PreviewSyncSource reads authenticated file metadata, and the remote ignore rule
// when needed. It does not apply files or advance the synchronization baseline.
func (repo *Repo) PreviewSyncSource(remote *entity.Index, context map[string]interface{}) (ret SyncSourcePreview, err error) {
	lock.Lock()
	defer lock.Unlock()
	local, err := repo.Latest()
	if err != nil {
		return ret, err
	}
	localFiles, err := repo.getFiles(local.Files)
	if err != nil {
		return ret, err
	}
	_, _, err = repo.getSyncCloudFiles(remote, context)
	if err != nil {
		return ret, err
	}
	remoteFiles, err := repo.getFiles(remote.Files)
	if err != nil {
		return ret, err
	}
	ret.LocalID, ret.LocalFiles = local.ID, len(localFiles)
	remoteIDs := map[string]bool{}
	for _, f := range remoteFiles {
		remoteIDs[f.ID] = true
	}
	localPaths := map[string]bool{}
	for _, f := range localFiles {
		localPaths[f.Path] = true
		if !remoteIDs[f.ID] {
			ret.UploadFiles++
		}
	}
	upserts, removes := repo.diffUpsertRemove(remoteFiles, localFiles, false)
	for _, f := range upserts {
		if localPaths[f.Path] {
			ret.OverwriteFiles++
		}
	}
	ret.RemoveFiles = len(removes)
	if repo.usesAssetDownloads() {
		matcher, matcherErr := repo.cloudAssetIgnoreMatcher(remoteFiles, context)
		if matcherErr != nil {
			return ret, matcherErr
		}
		for _, f := range removes {
			if matcher.MatchesPath(f.Path) {
				ret.RemoveFiles--
			}
		}
	}
	return
}
