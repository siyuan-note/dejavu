// DejaVu - Data snapshot and sync.
// Copyright (c) 2022-present, b3log.org
//
// DejaVu is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//         http://license.coscl.org.cn/MulanPSL2
//
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
//
// See the Mulan PSL v2 for more details.

package dejavu

import (
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/88250/gulu"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/dejavu/util"
)

func (repo *Repo) Latest() (ret *entity.Index, err error) {
	latest := filepath.Join(repo.Path, "refs", "latest")
	if !gulu.File.IsExist(latest) {
		ret = &entity.Index{ID: util.RandHash(), Message: "The first index", Created: time.Now().UnixMilli()}
		err = repo.store.PutIndex(ret)
		if nil != err {
			return
		}
		err = repo.UpdateLatest(ret.ID)
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
