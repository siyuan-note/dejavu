package dejavu

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/dejavu/util"
)

type workspaceLinkProbe struct {
	root string
	dirs map[string]bool
}

func (repo *Repo) newWorkspaceLinkProbe() *workspaceLinkProbe {
	root := filepath.Clean(repo.DataPath)
	return &workspaceLinkProbe{root: root, dirs: map[string]bool{root: false}}
}

func (p *workspaceLinkProbe) ancestor(abs string) (bool, error) {
	rel, err := filepath.Rel(p.root, abs)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return false, fmt.Errorf("invalid path within data directory [%s]", abs)
	}
	return p.dir(filepath.Dir(abs))
}

func (p *workspaceLinkProbe) dir(abs string) (bool, error) {
	if linked, ok := p.dirs[abs]; ok {
		return linked, nil
	}
	linked, err := p.dir(filepath.Dir(abs))
	if err != nil {
		return false, err
	}
	if !linked {
		linked, err = isWorkspaceLink(abs)
		if err != nil {
			return false, err
		}
	}
	p.dirs[abs] = linked
	return linked, nil
}

func isWorkspaceLink(abs string) (bool, error) {
	info, err := os.Lstat(abs)
	if isNoSuchFileOrDirErr(err) || errors.Is(err, syscall.ENOTDIR) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return info.Mode()&os.ModeSymlink != 0 || isDirectoryReparsePoint(info), nil
}

func (p *workspaceLinkProbe) linked(abs string) (bool, error) {
	if blocked, err := p.ancestor(abs); blocked || err != nil {
		return blocked, err
	}
	return isWorkspaceLink(abs)
}

func (repo *Repo) rejectWorkspaceLink(path string) error {
	linked, err := repo.newWorkspaceLinkProbe().linked(repo.absPath(path))
	if err != nil {
		return err
	}
	if linked {
		return fmt.Errorf("%w: workspace link at or above [%s]", ErrIndexFileChanged, path)
	}
	return nil
}

func (repo *Repo) syncLinkedPaths(localFiles []*entity.File, fileGroups ...[]*entity.File) (map[string]bool, error) {
	probe := repo.newWorkspaceLinkProbe()
	seen := map[string]bool{}
	linked := map[string]bool{}
	// 本地索引中的常规文件只需复核祖先，未被本地索引的路径还可能与联接本身重名。
	for _, file := range localFiles {
		blocked, err := probe.ancestor(repo.absPath(file.Path))
		if err != nil {
			return nil, fmt.Errorf("check sync path [%s]: %w", file.Path, err)
		}
		seen[file.Path] = true
		if blocked {
			linked[file.Path] = true
		}
	}
	for _, files := range fileGroups {
		for _, file := range files {
			if seen[file.Path] {
				continue
			}
			seen[file.Path] = true
			blocked, err := probe.linked(repo.absPath(file.Path))
			if err != nil {
				return nil, fmt.Errorf("check sync path [%s]: %w", file.Path, err)
			}
			if blocked {
				linked[file.Path] = true
			}
		}
	}
	return linked, nil
}

type syncLinkView struct {
	paths map[string]bool
	cloud []*entity.File
}

func (view syncLinkView) unlinked(files []*entity.File) []*entity.File {
	if len(view.paths) == 0 {
		return files
	}
	ret := make([]*entity.File, 0, len(files))
	for _, file := range files {
		if !view.paths[file.Path] {
			ret = append(ret, file)
		}
	}
	return ret
}

func buildSyncLinkView(files []*entity.File, linked map[string]bool) (view syncLinkView) {
	view.paths = linked
	for _, file := range files {
		if linked[file.Path] {
			view.cloud = append(view.cloud, file)
		}
	}
	return
}

func (view syncLinkView) publishedIndex(index *entity.Index, files []*entity.File) (*entity.Index, []*entity.File) {
	retained := view.unlinked(files)
	if len(retained) == len(files) && len(view.cloud) == 0 {
		return index, files
	}
	files = append(retained, view.cloud...)
	projected := *index
	projected.ID = util.RandHash()
	projected.Files = make([]string, 0, len(files))
	projected.Size = 0
	for _, source := range files {
		projected.Files = append(projected.Files, source.ID)
		projected.Size += source.Size
	}
	projected.Count = len(projected.Files)
	return &projected, files
}
