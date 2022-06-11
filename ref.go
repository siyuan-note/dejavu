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
)

func (repo *Repo) Latest() (ret *Index, err error) {
	latest := filepath.Join(repo.Path, "refs", "latest")
	if !gulu.File.IsExist(latest) {
		ret = &Index{Hash: RandHash(), Message: "Init commit", Created: time.Now().UnixMilli()}
		err = repo.store.PutIndex(ret)
		if nil != err {
			return
		}
		err = repo.UpdateLatest(ret.ID())
		return
	}

	data, err := os.ReadFile(latest)
	if nil != err {
		return
	}
	hash := string(data)
	ret, err = repo.store.GetIndex(hash)
	return
}

func (repo *Repo) UpdateLatest(id string) (err error) {
	refs := filepath.Join(repo.Path, "refs")
	err = os.MkdirAll(refs, 0755)
	if nil != err {
		return
	}
	err = gulu.File.WriteFileSafer(filepath.Join(refs, "latest"), []byte(id), 0644)
	return
}

func (repo *Repo) GetTag(tag string) (id string, err error) {
	if !gulu.File.IsValidFilename(tag) {
		err = errors.New("invalid tag name")
	}
	tag = filepath.Join(repo.Path, "refs", "tags", tag)
	if !gulu.File.IsExist(tag) {
		err = errors.New("tag not found")
	}
	data, err := os.ReadFile(tag)
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
