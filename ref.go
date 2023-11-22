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
	"os"
	"path/filepath"
	"time"

	"github.com/88250/gulu"
	"github.com/dustin/go-humanize"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/filelock"
	"github.com/siyuan-note/logging"
)

var ErrNotFoundIndex = errors.New("not found index")

func (repo *Repo) Latest() (ret *entity.Index, err error) {
	latest := filepath.Join(repo.Path, "refs", "latest")
	if !filelock.IsExist(latest) {
		err = ErrNotFoundIndex
		return
	}

	data, err := os.ReadFile(latest)
	if nil != err {
		logging.LogErrorf("read latest index [%s] failed: %s", latest, err)
		return
	}
	hash := string(data)
	ret, err = repo.store.GetIndex(hash)
	if nil != err {
		logging.LogErrorf("get latest index [%s] failed: %s", hash, err)
		return
	}
	logging.LogInfof("got local latest [device=%s/%s, id=%s, files=%d, size=%s, created=%s]", ret.SystemID, ret.SystemOS, ret.ID, len(ret.Files), humanize.Bytes(uint64(ret.Size)), time.UnixMilli(ret.Created).Format("2006-01-02 15:04:05"))
	return
}

func (repo *Repo) UpdateLatest(index *entity.Index) (err error) {
	refs := filepath.Join(repo.Path, "refs")
	err = os.MkdirAll(refs, 0755)
	if nil != err {
		return
	}
	err = gulu.File.WriteFileSafer(filepath.Join(refs, "latest"), []byte(index.ID), 0644)
	if nil != err {
		return
	}
	logging.LogInfof("updated local latest to [device=%s/%s, id=%s, files=%d, size=%s, created=%s]", index.SystemID, index.SystemOS, index.ID, len(index.Files), humanize.Bytes(uint64(index.Size)), time.UnixMilli(index.Created).Format("2006-01-02 15:04:05"))
	return
}

func (repo *Repo) GetTag(tag string) (id string, err error) {
	if !gulu.File.IsValidFilename(tag) {
		err = errors.New("invalid tag name")
	}
	tag = filepath.Join(repo.Path, "refs", "tags", tag)
	if !filelock.IsExist(tag) {
		err = errors.New("tag not found")
	}
	data, err := filelock.ReadFile(tag)
	if nil != err {
		return
	}
	id = string(data)
	return
}

func (repo *Repo) AddTag(id, tag string) (err error) {
	if !gulu.File.IsValidFilename(tag) {
		return errors.New("invalid tag name")
	}

	_, err = repo.store.GetIndex(id)
	if nil != err {
		return
	}

	tags := filepath.Join(repo.Path, "refs", "tags")
	if err = os.MkdirAll(tags, 0755); nil != err {
		return
	}
	tag = filepath.Join(tags, tag)
	err = gulu.File.WriteFileSafer(tag, []byte(id), 0644)
	return
}

func (repo *Repo) RemoveTag(tag string) (err error) {
	tag = filepath.Join(repo.Path, "refs", "tags", tag)
	if !gulu.File.IsExist(tag) {
		return
	}

	err = os.Remove(tag)
	return
}
