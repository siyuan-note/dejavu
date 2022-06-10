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

// Upsert 比较 left 多于/变动 right 的文件。
func Upsert(left, right []*File) (ret []*File) {
	l := map[string]*File{}
	r := map[string]*File{}
	for _, f := range left {
		l[f.ID()] = f
	}
	for _, f := range right {
		r[f.ID()] = f
	}

	for lID := range l {
		if _, ok := r[lID]; !ok {
			ret = append(ret, l[lID])
		}
	}
	return
}
