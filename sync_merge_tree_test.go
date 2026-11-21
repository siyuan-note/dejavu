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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/88250/lute"
	"github.com/88250/lute/ast"
	"github.com/restic/chunker"
	"github.com/siyuan-note/dataparser"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/dejavu/util"
)

func syPara(id, text, updated string) string {
	return `{"ID":"` + id + `","Type":"NodeParagraph","Properties":{"id":"` + id + `","updated":"` + updated + `"},"Children":[{"Type":"NodeText","Data":"` + text + `"}]}`
}

func syHeading(id, text string) string {
	return `{"ID":"` + id + `","Type":"NodeHeading","HeadingLevel":2,"Properties":{"id":"` + id + `","updated":"20240101000000"},"Children":[{"Type":"NodeText","Data":"` + text + `"}]}`
}

func syListItem(id string, blocks ...string) string {
	return `{"ID":"` + id + `","Type":"NodeListItem","ListData":{"BulletChar":42,"Marker":"Kg=="},"Properties":{"id":"` + id + `","updated":"20240101000000"},"Children":[` + strings.Join(blocks, ",") + `]}`
}

func syList(id string, items ...string) string {
	return `{"ID":"` + id + `","Type":"NodeList","ListData":{},"Properties":{"id":"` + id + `","updated":"20240101000000"},"Children":[` + strings.Join(items, ",") + `]}`
}

func syQuote(id string, blocks ...string) string {
	return `{"ID":"` + id + `","Type":"NodeBlockquote","Properties":{"id":"` + id + `","updated":"20240101000000"},"Children":[{"Type":"NodeBlockquoteMarker","Data":">"},` + strings.Join(blocks, ",") + `]}`
}

func syDoc(blocks ...string) []byte {
	return []byte(`{"ID":"20240101000000-doc0000","Spec":"1","Type":"NodeDocument","Properties":{"id":"20240101000000-doc0000","title":"t","type":"doc","updated":"20240101000000"},"Children":[` + strings.Join(blocks, ",") + `]}`)
}

// syOutline 把文档渲染成 "ID:text" 的块序列，便于断言合并结果的结构。
func syOutline(t *testing.T, data []byte) []string {
	t.Helper()
	tree, err := dataparser.ParseJSONWithoutFix(data, lute.New().ParseOptions)
	if nil != err {
		t.Fatalf("parse merged doc failed: %s\n%s", err, data)
	}
	var ret []string
	ast.Walk(tree.Root, func(n *ast.Node, entering bool) ast.WalkStatus {
		if !entering || !n.IsBlock() || ast.NodeDocument == n.Type {
			return ast.WalkContinue
		}
		if n.IsContainerBlock() {
			ret = append(ret, n.ID+":"+n.Type.String())
			return ast.WalkContinue
		}
		ret = append(ret, n.ID+":"+n.Text())
		return ast.WalkContinue
	})
	return ret
}

func mustMerge(t *testing.T, base, local, cloud []byte) []byte {
	t.Helper()
	merged, ok, err := mergeSyDocuments(base, local, cloud, lute.New())
	if nil != err {
		t.Fatalf("merge failed: %s", err)
	}
	if !ok {
		t.Fatalf("expected merge to succeed")
	}
	return merged
}

func mustConflict(t *testing.T, base, local, cloud []byte) {
	t.Helper()
	merged, ok, err := mergeSyDocuments(base, local, cloud, lute.New())
	if nil != err {
		t.Fatalf("merge failed: %s", err)
	}
	if ok {
		t.Fatalf("expected merge conflict, got merged doc:\n%s", merged)
	}
}

func assertOutline(t *testing.T, got, want []string) {
	t.Helper()
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("outline mismatch\n got: %v\nwant: %v", got, want)
	}
}

const (
	p1 = "20240101000001-aaaaaa1"
	p2 = "20240101000002-aaaaaa2"
	p3 = "20240101000003-aaaaaa3"
	p4 = "20240101000004-aaaaaa4"
	p5 = "20240101000005-aaaaaa5"
	l1 = "20240101000010-list001"
	i1 = "20240101000011-item001"
	i2 = "20240101000012-item002"
	q1 = "20240101000020-quote01"
)

func TestDiff3Sequence(t *testing.T) {
	tests := []struct {
		name               string
		base, local, cloud []string
		want               []string
		ok                 bool
	}{
		{"unchanged", []string{"a", "b", "c"}, []string{"a", "b", "c"}, []string{"a", "b", "c"}, []string{"a", "b", "c"}, true},
		{"local inserts, cloud unchanged", []string{"a", "b"}, []string{"a", "x", "b"}, []string{"a", "b"}, []string{"a", "x", "b"}, true},
		{"cloud inserts, local unchanged", []string{"a", "b"}, []string{"a", "b"}, []string{"a", "b", "y"}, []string{"a", "b", "y"}, true},
		{"both insert in different places", []string{"a", "b", "c"}, []string{"x", "a", "b", "c"}, []string{"a", "b", "c", "y"}, []string{"x", "a", "b", "c", "y"}, true},
		{"local deletes, cloud inserts elsewhere", []string{"a", "b", "c"}, []string{"a", "c"}, []string{"a", "b", "c", "y"}, []string{"a", "c", "y"}, true},
		{"both delete same", []string{"a", "b", "c"}, []string{"a", "c"}, []string{"a", "c"}, []string{"a", "c"}, true},
		{"both insert same", []string{"a"}, []string{"a", "x"}, []string{"a", "x"}, []string{"a", "x"}, true},
		{"both insert different in same gap", []string{"a", "b"}, []string{"a", "x", "b"}, []string{"a", "y", "b"}, nil, false},
		{"local reorders, cloud unchanged", []string{"a", "b", "c"}, []string{"c", "a", "b"}, []string{"a", "b", "c"}, []string{"c", "a", "b"}, true},
		{"both reorder differently", []string{"a", "b", "c"}, []string{"b", "a", "c"}, []string{"a", "c", "b"}, nil, false},
		{"empty base, both add different", nil, []string{"x"}, []string{"y"}, nil, false},
		{"empty base, only local adds", nil, []string{"x"}, nil, []string{"x"}, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := diff3Sequence(test.base, test.local, test.cloud)
			if ok != test.ok {
				t.Fatalf("ok=%v, want %v (got %v)", ok, test.ok, got)
			}
			if ok && !equalStrings(got, test.want) {
				t.Fatalf("got %v, want %v", got, test.want)
			}
		})
	}
}

func TestUniqueLCSMatch(t *testing.T) {
	match := uniqueLCSMatch([]string{"a", "b", "c", "d"}, []string{"b", "a", "c", "x", "d"})
	// 最长公共子序列为 a c d 或 b c d，长度 3
	if 3 != len(match) {
		t.Fatalf("expected 3 matches, got %v", match)
	}
	if match[2] != 2 || match[3] != 4 {
		t.Fatalf("expected c->2, d->4, got %v", match)
	}
}

func TestMergeSyDocumentsIndependentEdits(t *testing.T) {
	base := syDoc(syPara(p1, "first", "20240101000000"), syPara(p2, "second", "20240101000000"), syPara(p3, "third", "20240101000000"))
	// 本地改了第二段
	local := syDoc(syPara(p1, "first", "20240101000000"), syPara(p2, "second changed", "20240102000000"), syPara(p3, "third", "20240101000000"))
	// 云端在第一段后插入了新段落并改了第三段
	cloud := syDoc(syPara(p1, "first", "20240101000000"), syPara(p4, "inserted", "20240103000000"), syPara(p2, "second", "20240101000000"), syPara(p3, "third from cloud", "20240103000000"))

	merged := mustMerge(t, base, local, cloud)
	assertOutline(t, syOutline(t, merged), []string{p1 + ":first", p4 + ":inserted", p2 + ":second changed", p3 + ":third from cloud"})

	// 输出必须是制表符缩进的合法 JSON，且保留块属性
	if !strings.HasPrefix(string(merged), "{\n\t\"ID\"") {
		t.Fatalf("merged doc is not tab indented:\n%s", merged)
	}
	var generic map[string]interface{}
	if err := json.Unmarshal(merged, &generic); nil != err {
		t.Fatalf("merged doc is not valid json: %s", err)
	}
	if !strings.Contains(string(merged), `"updated": "20240102000000"`) {
		t.Fatalf("expected local block updated attr to be kept:\n%s", merged)
	}
}

func TestMergeSyDocumentsSameBlockConflicts(t *testing.T) {
	base := syDoc(syPara(p1, "first", "20240101000000"), syPara(p2, "second", "20240101000000"))
	local := syDoc(syPara(p1, "first", "20240101000000"), syPara(p2, "second from local", "20240102000000"))
	cloud := syDoc(syPara(p1, "first", "20240101000000"), syPara(p2, "second from cloud", "20240103000000"))
	mustConflict(t, base, local, cloud)
}

func TestMergeSyDocumentsSameEditBothSides(t *testing.T) {
	base := syDoc(syPara(p1, "first", "20240101000000"))
	local := syDoc(syPara(p1, "first edited", "20240102000000"))
	cloud := syDoc(syPara(p1, "first edited", "20240103000000"))
	merged := mustMerge(t, base, local, cloud)
	assertOutline(t, syOutline(t, merged), []string{p1 + ":first edited"})
}

func TestMergeSyDocumentsOnlyUpdatedAttrDiffers(t *testing.T) {
	base := syDoc(syPara(p1, "first", "20240101000000"), syPara(p2, "second", "20240101000000"))
	local := syDoc(syPara(p1, "first", "20240102000000"), syPara(p2, "second", "20240101000000"))
	cloud := syDoc(syPara(p1, "first", "20240101000000"), syPara(p2, "second changed", "20240103000000"))
	merged := mustMerge(t, base, local, cloud)
	assertOutline(t, syOutline(t, merged), []string{p1 + ":first", p2 + ":second changed"})
}

func TestMergeSyDocumentsDeleteVersusEditConflicts(t *testing.T) {
	base := syDoc(syPara(p1, "first", "20240101000000"), syPara(p2, "second", "20240101000000"))
	local := syDoc(syPara(p1, "first", "20240101000000"))                                                // 本地删除了第二段
	cloud := syDoc(syPara(p1, "first", "20240101000000"), syPara(p2, "second edited", "20240103000000")) // 云端改了第二段
	mustConflict(t, base, local, cloud)
	mustConflict(t, base, cloud, local)
}

func TestMergeSyDocumentsDeleteVersusUnchangedApplies(t *testing.T) {
	base := syDoc(syPara(p1, "first", "20240101000000"), syPara(p2, "second", "20240101000000"), syPara(p3, "third", "20240101000000"))
	local := syDoc(syPara(p1, "first", "20240101000000"), syPara(p3, "third", "20240101000000"))
	cloud := syDoc(syPara(p1, "first", "20240101000000"), syPara(p2, "second", "20240101000000"), syPara(p3, "third edited", "20240103000000"))
	merged := mustMerge(t, base, local, cloud)
	assertOutline(t, syOutline(t, merged), []string{p1 + ":first", p3 + ":third edited"})
}

func TestMergeSyDocumentsNestedContainers(t *testing.T) {
	base := syDoc(
		syHeading(p1, "title"),
		syList(l1, syListItem(i1, syPara(p2, "item one", "20240101000000")), syListItem(i2, syPara(p3, "item two", "20240101000000"))),
		syQuote(q1, syPara(p4, "quoted", "20240101000000")),
	)
	// 本地改了第一个列表项的段落
	local := syDoc(
		syHeading(p1, "title"),
		syList(l1, syListItem(i1, syPara(p2, "item one edited", "20240102000000")), syListItem(i2, syPara(p3, "item two", "20240101000000"))),
		syQuote(q1, syPara(p4, "quoted", "20240101000000")),
	)
	// 云端在第二个列表项里追加了段落并改了引述
	cloud := syDoc(
		syHeading(p1, "title"),
		syList(l1, syListItem(i1, syPara(p2, "item one", "20240101000000")), syListItem(i2, syPara(p3, "item two", "20240101000000"), syPara(p5, "item two more", "20240103000000"))),
		syQuote(q1, syPara(p4, "quoted edited", "20240103000000")),
	)
	merged := mustMerge(t, base, local, cloud)
	assertOutline(t, syOutline(t, merged), []string{
		p1 + ":title",
		l1 + ":NodeList", i1 + ":NodeListItem", p2 + ":item one edited", i2 + ":NodeListItem", p3 + ":item two", p5 + ":item two more",
		q1 + ":NodeBlockquote", p4 + ":quoted edited",
	})
	// 引述标记节点不能丢
	if !strings.Contains(string(merged), `"Type": "NodeBlockquoteMarker"`) {
		t.Fatalf("blockquote marker lost:\n%s", merged)
	}
}

func TestMergeSyDocumentsContainerAttrAndChildren(t *testing.T) {
	// 本地改了列表项的任务勾选状态（自身属性），云端改了列表项里的段落（子块）
	itemChecked := `{"ID":"` + i1 + `","Type":"NodeListItem","ListData":{"Typ":3,"Checked":true,"BulletChar":42,"Marker":"Kg=="},"Properties":{"id":"` + i1 + `","updated":"20240102000000"},"Children":[` + syPara(p2, "task", "20240101000000") + `]}`
	itemUnchecked := `{"ID":"` + i1 + `","Type":"NodeListItem","ListData":{"Typ":3,"BulletChar":42,"Marker":"Kg=="},"Properties":{"id":"` + i1 + `","updated":"20240101000000"},"Children":[` + syPara(p2, "task", "20240101000000") + `]}`
	itemEdited := `{"ID":"` + i1 + `","Type":"NodeListItem","ListData":{"Typ":3,"BulletChar":42,"Marker":"Kg=="},"Properties":{"id":"` + i1 + `","updated":"20240103000000"},"Children":[` + syPara(p2, "task renamed", "20240103000000") + `]}`
	base := syDoc(syList(l1, itemUnchecked))
	local := syDoc(syList(l1, itemChecked))
	cloud := syDoc(syList(l1, itemEdited))
	merged := mustMerge(t, base, local, cloud)
	assertOutline(t, syOutline(t, merged), []string{l1 + ":NodeList", i1 + ":NodeListItem", p2 + ":task renamed"})
	if !strings.Contains(string(merged), `"Checked": true`) {
		t.Fatalf("expected local checked state to be kept:\n%s", merged)
	}
}

func TestMergeSyDocumentsDocumentTitle(t *testing.T) {
	base := syDoc(syPara(p1, "first", "20240101000000"))
	local := []byte(strings.Replace(string(syDoc(syPara(p1, "first", "20240101000000"))), `"title":"t"`, `"title":"renamed"`, 1))
	cloud := syDoc(syPara(p1, "first edited", "20240103000000"))
	merged := mustMerge(t, base, local, cloud)
	if !strings.Contains(string(merged), `"title": "renamed"`) {
		t.Fatalf("expected local title to be kept:\n%s", merged)
	}
	assertOutline(t, syOutline(t, merged), []string{p1 + ":first edited"})
}

func TestMergeSyDocumentsInvalidJSON(t *testing.T) {
	_, ok, err := mergeSyDocuments([]byte("not json"), syDoc(), syDoc(), lute.New())
	if nil == err || ok {
		t.Fatalf("expected parse error, got ok=%v err=%v", ok, err)
	}
}

const (
	i3 = "20240101000013-item003"
	l2 = "20240101000014-list002"
	l3 = "20240101000015-list003"
	t1 = "20240101000030-tabs001"
	a1 = "20240101000031-tabi001"
	a2 = "20240101000032-tabi002"
)

func syTabItem(id, title string, blocks ...string) string {
	return `{"ID":"` + id + `","Type":"NodeTabItem","TabItemTitle":"` + title + `","Properties":{"id":"` + id + `","updated":"20240101000000"},"Children":[` + strings.Join(blocks, ",") + `]}`
}

func syTabs(id, active string, items ...string) string {
	return `{"ID":"` + id + `","Type":"NodeTabs","Properties":{"id":"` + id + `","updated":"20240101000000","tabs-active-id":"` + active + `","tabs-position":"top"},"Children":[` + strings.Join(items, ",") + `]}`
}

func syDocSpec(spec string, blocks ...string) []byte {
	return []byte(`{"ID":"20240101000000-doc0000","Spec":"` + spec + `","Type":"NodeDocument","Properties":{"id":"20240101000000-doc0000","title":"t","type":"doc","updated":"20240101000000"},"Children":[` + strings.Join(blocks, ",") + `]}`)
}

func TestMergeSyDocumentsRejectsUnsupportedSpec(t *testing.T) {
	future := syDocSpec("4", syPara(p1, "first", "20240101000000"))
	_, ok, err := mergeSyDocuments(future, future, future, lute.New())
	if !errors.Is(err, errSyUnsupportedSpec) || ok {
		t.Fatalf("expected unsupported spec error, got ok=%v err=%v", ok, err)
	}
}

func TestMergeSyDocumentsRejectsUnknownNodeType(t *testing.T) {
	unknown := syDoc(`{"ID":"` + p1 + `","Type":"NodeFromTheFuture","Properties":{"id":"` + p1 + `","updated":"20240101000000"},"Children":[{"Type":"NodeText","Data":"x"}]}`)
	_, ok, err := mergeSyDocuments(unknown, unknown, unknown, lute.New())
	if !errors.Is(err, errSyUnknownNodeType) || ok {
		t.Fatalf("expected unknown node type error, got ok=%v err=%v", ok, err)
	}
}

func TestMergeSyDocumentsRejectsLossyRoundTrip(t *testing.T) {
	// 段落上带了当前版本不认识的字段，重新渲染会丢掉它，这种文档不能自动合并
	lossy := syDoc(`{"ID":"` + p1 + `","Type":"NodeParagraph","FieldFromTheFuture":1,"Properties":{"id":"` + p1 + `","updated":"20240101000000"},"Children":[{"Type":"NodeText","Data":"x"}]}`)
	_, ok, err := mergeSyDocuments(lossy, lossy, lossy, lute.New())
	if !errors.Is(err, errSyLossyRoundTrip) || ok {
		t.Fatalf("expected lossy round trip error, got ok=%v err=%v", ok, err)
	}
}

func TestMergeSyDocumentsToleratesRuntimeAttrs(t *testing.T) {
	// refcount / av-names 是渲染时固定剔除的运行态属性，不应阻止合并
	withRuntime := func(text, updated string) string {
		return `{"ID":"` + p1 + `","Type":"NodeParagraph","Properties":{"id":"` + p1 + `","updated":"` + updated + `","refcount":"2"},"Children":[{"Type":"NodeText","Data":"` + text + `"}]}`
	}
	base := syDoc(withRuntime("first", "20240101000000"), syPara(p2, "second", "20240101000000"))
	local := syDoc(withRuntime("first edited", "20240102000000"), syPara(p2, "second", "20240101000000"))
	cloud := syDoc(withRuntime("first", "20240101000000"), syPara(p2, "second edited", "20240103000000"))
	merged := mustMerge(t, base, local, cloud)
	assertOutline(t, syOutline(t, merged), []string{p1 + ":first edited", p2 + ":second edited"})
}

func TestMergeSyDocumentsMoveAcrossContainers(t *testing.T) {
	item := func(id, text string) string {
		return syListItem(id, syPara(text, "item "+id[len(id)-3:], "20240101000000"))
	}
	base := syDoc(syList(l1, item(i1, p2), item(i2, p3)), syList(l2, item(i3, p4)))

	// 只有本地把 i2 从第一个列表移到第二个列表，云端没动：可以合并
	localMoved := syDoc(syList(l1, item(i1, p2)), syList(l2, item(i3, p4), item(i2, p3)))
	merged := mustMerge(t, base, localMoved, base)
	assertOutline(t, syOutline(t, merged), []string{
		l1 + ":NodeList", i1 + ":NodeListItem", p2 + ":item 001",
		l2 + ":NodeList", i3 + ":NodeListItem", p4 + ":item 003", i2 + ":NodeListItem", p3 + ":item 002",
	})

	// 两端把 i2 移到了同一个列表的不同位置：冲突，不能出现两个相同 ID 的块
	cloudMovedToOtherPosition := syDoc(syList(l1, item(i1, p2)), syList(l2, item(i2, p3), item(i3, p4)))
	mustConflict(t, base, localMoved, cloudMovedToOtherPosition)

	// 两端把 i2 移到了不同的容器：冲突
	cloudMovedElsewhere := syDoc(syList(l1, item(i1, p2)), syList(l2, item(i3, p4)), syList(l3, item(i2, p3)))
	mustConflict(t, base, localMoved, cloudMovedElsewhere)

	// 本地移动、云端删除：冲突
	cloudDeleted := syDoc(syList(l1, item(i1, p2)), syList(l2, item(i3, p4)))
	mustConflict(t, base, localMoved, cloudDeleted)

	// 两端移到同一个地方：合并
	merged = mustMerge(t, base, localMoved, localMoved)
	assertOutline(t, syOutline(t, merged), []string{
		l1 + ":NodeList", i1 + ":NodeListItem", p2 + ":item 001",
		l2 + ":NodeList", i3 + ":NodeListItem", p4 + ":item 003", i2 + ":NodeListItem", p3 + ":item 002",
	})
}

func TestMergeSyDocumentsTabs(t *testing.T) {
	base := syDocSpec("3", syTabs(t1, a1, syTabItem(a1, "One", syPara(p1, "tab one", "20240101000000")), syTabItem(a2, "Two", syPara(p2, "tab two", "20240101000000"))))
	// 本地改了第一个页签的内容并切换了激活页签（容器属性）
	local := syDocSpec("3", syTabs(t1, a2, syTabItem(a1, "One", syPara(p1, "tab one edited", "20240102000000")), syTabItem(a2, "Two", syPara(p2, "tab two", "20240101000000"))))
	// 云端在第二个页签里追加了段落并改了页签标题
	cloud := syDocSpec("3", syTabs(t1, a1, syTabItem(a1, "One", syPara(p1, "tab one", "20240101000000")), syTabItem(a2, "Two renamed", syPara(p2, "tab two", "20240101000000"), syPara(p3, "more", "20240103000000"))))
	merged := mustMerge(t, base, local, cloud)
	assertOutline(t, syOutline(t, merged), []string{
		t1 + ":NodeTabs", a1 + ":NodeTabItem", p1 + ":tab one edited", a2 + ":NodeTabItem", p2 + ":tab two", p3 + ":more",
	})
	for _, want := range []string{`"Spec": "3"`, `"tabs-active-id": "` + a2 + `"`, `"TabItemTitle": "Two renamed"`} {
		if !strings.Contains(string(merged), want) {
			t.Fatalf("expected %s in merged doc:\n%s", want, merged)
		}
	}
}

// 合并结果和本地修改落在同一秒时，必须入库为新的文件版本，且不能覆盖已有版本。
func TestIndexMergedFileSameSecond(t *testing.T) {
	tempDir := t.TempDir()
	dataPath := filepath.Join(tempDir, "data")
	if err := os.MkdirAll(dataPath, 0755); nil != err {
		t.Fatal(err)
	}
	repo, err := NewRepo(dataPath, filepath.Join(tempDir, "repo"), filepath.Join(tempDir, "history"), filepath.Join(tempDir, "temp"),
		"device", "Device", "linux", []byte("0123456789abcdef0123456789abcdef"), nil, nil)
	if nil != err {
		t.Fatal(err)
	}

	relPath := "/" + filepath.Base(filepath.Dir(tempDir)) + ".sy"
	absPath := filepath.Join(dataPath, relPath)
	sameSecond := time.Now().Add(time.Hour).Truncate(time.Second)
	localData := []byte("local content")
	if err = os.WriteFile(absPath, localData, 0644); nil != err {
		t.Fatal(err)
	}
	if err = os.Chtimes(absPath, sameSecond, sameSecond); nil != err {
		t.Fatal(err)
	}
	if _, err = repo.Index("local edit", false, map[string]interface{}{}); nil != err {
		t.Fatal(err)
	}
	latest, err := repo.Latest()
	if nil != err {
		t.Fatal(err)
	}
	files, err := repo.GetFiles(latest)
	if nil != err || 1 != len(files) {
		t.Fatalf("expected one indexed file, got %d (%v)", len(files), err)
	}
	local := files[0]

	// 合并结果与本地修改发生在同一秒时必须有不同的 ID。
	mergedData := []byte("merged content")
	if entity.NewFile(local.Path, int64(len(mergedData)), sameSecond.Add(300*time.Millisecond).UnixMilli()).ID != local.ID {
		t.Fatalf("test setup must reproduce the same-second ID collision")
	}

	merged, err := repo.indexMergedFileData(local.Path, mergedData, []*entity.File{local})
	if nil != err {
		t.Fatal(err)
	}
	if merged.ID == local.ID {
		t.Fatalf("merged version reused the local version ID [%s]", local.ID)
	}
	if merged.SecUpdated() <= local.SecUpdated() {
		t.Fatalf("merged version updated [%d] must be later than local [%d]", merged.SecUpdated(), local.SecUpdated())
	}

	readVersion := func(file *entity.File) string {
		stored, getErr := repo.store.GetFile(file.ID)
		if nil != getErr {
			t.Fatal(getErr)
		}
		var buf []byte
		for _, id := range stored.Chunks {
			chunk, chunkErr := repo.store.GetChunk(id)
			if nil != chunkErr {
				t.Fatal(chunkErr)
			}
			buf = append(buf, chunk.Data...)
		}
		return string(buf)
	}
	if got := readVersion(merged); got != string(mergedData) {
		t.Fatalf("merged version content = %q, want %q", got, mergedData)
	}
	if got := readVersion(local); got != string(localData) {
		t.Fatalf("existing local version was changed: %q", got)
	}

	// 暂存不会提前覆盖本地文件；正常 checkout 后重新扫描得到同一个合并版本。
	if data, readErr := os.ReadFile(absPath); nil != readErr || !bytes.Equal(data, localData) {
		t.Fatalf("staging changed local data: %q (%v)", data, readErr)
	}
	if err = repo.checkoutFile(merged, dataPath, 1, 1, nil, local); nil != err {
		t.Fatal(err)
	}
	reindexed, err := repo.Index("rescan", false, map[string]interface{}{})
	if nil != err {
		t.Fatal(err)
	}
	files, err = repo.GetFiles(reindexed)
	if nil != err || 1 != len(files) || files[0].ID != merged.ID {
		t.Fatalf("rescan produced %v, want merged version [%s] (%v)", files, merged.ID, err)
	}
}

// Spec 4 的表格单元格富文本（TableCellRich）超出当前支持范围，必须拒绝合并，交给文件级冲突原样保留。
func TestMergeSyDocumentsRejectsSpec4TableCellRich(t *testing.T) {
	read := func(name string) []byte {
		data, err := os.ReadFile(filepath.Join("test", "sync", "testdata", "cases", "structured-merge", "spec4", name))
		if nil != err {
			t.Fatal(err)
		}
		return data
	}
	base, local, cloud := read("base.sy"), read("local.sy"), read("cloud.sy")
	if !strings.Contains(string(cloud), `"TableCellRich"`) || !strings.Contains(string(cloud), `"custom-sy-table-rich": "1"`) {
		t.Fatalf("fixture must contain a real table cell rich envelope")
	}
	merged, ok, err := mergeSyDocuments(base, local, cloud, lute.New())
	if !errors.Is(err, errSyUnsupportedSpec) || ok || nil != merged {
		t.Fatalf("expected unsupported spec rejection, got ok=%v err=%v", ok, err)
	}
}

// 候选 ID 对应的对象存在但已损坏时，不能把它误判成"ID 不存在"：
// 那样入库会失败，合并结果会丢失。必须换用下一个未占用的时间。
func TestIndexMergedFileSkipsCorruptObject(t *testing.T) {
	tempDir := t.TempDir()
	dataPath := filepath.Join(tempDir, "data")
	if err := os.MkdirAll(dataPath, 0755); nil != err {
		t.Fatal(err)
	}
	repo, err := NewRepo(dataPath, filepath.Join(tempDir, "repo"), filepath.Join(tempDir, "history"), filepath.Join(tempDir, "temp"),
		"device", "Device", "linux", []byte("0123456789abcdef0123456789abcdef"), nil, nil)
	if nil != err {
		t.Fatal(err)
	}

	relPath := "/" + filepath.Base(filepath.Dir(tempDir)) + ".sy"
	absPath := filepath.Join(dataPath, relPath)
	sameSecond := time.Now().Add(time.Hour).Truncate(time.Second)
	localData := []byte("local content")
	if err = os.WriteFile(absPath, localData, 0644); nil != err {
		t.Fatal(err)
	}
	if err = os.Chtimes(absPath, sameSecond, sameSecond); nil != err {
		t.Fatal(err)
	}
	if _, err = repo.Index("local edit", false, map[string]interface{}{}); nil != err {
		t.Fatal(err)
	}
	latest, err := repo.Latest()
	if nil != err {
		t.Fatal(err)
	}
	files, err := repo.GetFiles(latest)
	if nil != err || 1 != len(files) {
		t.Fatalf("expected one indexed file, got %d (%v)", len(files), err)
	}
	local := files[0]

	mergedData := []byte("merged content")

	// 第一个候选 ID 上放一个损坏的对象
	candidate := entity.NewFile(local.Path, int64(len(mergedData)), (local.Updated/1000+1)*1000)
	corruptDir, corruptFile := repo.store.AbsPath(candidate.ID)
	if err = os.MkdirAll(corruptDir, 0755); nil != err {
		t.Fatal(err)
	}
	if err = os.WriteFile(corruptFile, []byte("corrupted object"), 0644); nil != err {
		t.Fatal(err)
	}
	merged, err := repo.indexMergedFileData(local.Path, mergedData, []*entity.File{local})
	if nil != err {
		t.Fatalf("indexing must skip the corrupted candidate: %s", err)
	}
	if merged.ID == candidate.ID {
		t.Fatalf("merged version reused the corrupted candidate ID [%s]", candidate.ID)
	}
	corruptData, err := os.ReadFile(corruptFile)
	if nil != err || string(corruptData) != "corrupted object" {
		t.Fatalf("corrupt object was changed: %q (%v)", corruptData, err)
	}
	if merged.SecUpdated() <= local.SecUpdated() {
		t.Fatalf("merged version updated [%d] must be later than local [%d]", merged.SecUpdated(), local.SecUpdated())
	}

	stored, err := repo.store.GetFile(merged.ID)
	if nil != err {
		t.Fatal(err)
	}
	var buf []byte
	for _, id := range stored.Chunks {
		chunk, chunkErr := repo.store.GetChunk(id)
		if nil != chunkErr {
			t.Fatal(chunkErr)
		}
		buf = append(buf, chunk.Data...)
	}
	if string(buf) != string(mergedData) {
		t.Fatalf("merged version content = %q, want %q", buf, mergedData)
	}
}

// 大文档暂存使用与普通索引相同的分块方式，checkout 后重新索引仍引用同一版本。
func TestStagedMergedFileUsesIndexChunking(t *testing.T) {
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "data")
	if err := os.MkdirAll(dataPath, 0755); err != nil {
		t.Fatal(err)
	}
	repo, err := NewRepo(dataPath, filepath.Join(dir, "repo"), filepath.Join(dir, "history"), filepath.Join(dir, "temp"),
		"device", "Device", "linux", []byte("0123456789abcdef0123456789abcdef"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	relPath := "/" + filepath.Base(filepath.Dir(dir)) + ".sy"
	absPath := repo.absPath(relPath)
	if err = os.WriteFile(absPath, []byte("local"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err = repo.Index("local", false, nil); err != nil {
		t.Fatal(err)
	}
	localIndex, err := repo.Latest()
	if err != nil {
		t.Fatal(err)
	}
	files, err := repo.GetFiles(localIndex)
	if err != nil || len(files) != 1 {
		t.Fatalf("expected local version: %v (%v)", files, err)
	}
	data := make([]byte, 2*chunker.MinSize+123)
	for i := range data {
		data[i] = byte((i*37 + i/251) % 256)
	}
	merged, err := repo.indexMergedFileData(relPath, data, files)
	if err != nil {
		t.Fatal(err)
	}
	if err = repo.checkoutFile(merged, dataPath, 1, 1, nil, files[0]); err != nil {
		t.Fatal(err)
	}
	indexed, err := repo.Index("rescan", false, nil)
	if err != nil {
		t.Fatal(err)
	}
	files, err = repo.GetFiles(indexed)
	if err != nil || len(files) != 1 || files[0].ID != merged.ID {
		t.Fatalf("rescan did not reuse staged version: %v (%v)", files, err)
	}
	got, err := repo.openFile(files[0])
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("reindexed data differs: %v", err)
	}
}

// 合并结果入库失败时必须向上传递错误，且数据目录保持原样。
func TestMergeStructuredSyncFileIndexFailurePreservesLocal(t *testing.T) {
	tempDir := t.TempDir()
	dataPath := filepath.Join(tempDir, "data")
	if err := os.MkdirAll(dataPath, 0755); nil != err {
		t.Fatal(err)
	}
	repo, err := NewRepo(dataPath, filepath.Join(tempDir, "repo"), filepath.Join(tempDir, "history"), filepath.Join(tempDir, "temp"),
		"device", "Device", "linux", []byte("0123456789abcdef0123456789abcdef"), nil, nil)
	if nil != err {
		t.Fatal(err)
	}

	base := syDoc(syPara(p1, "first", "20240101000000"), syPara(p2, "second", "20240101000000"))
	local := syDoc(syPara(p1, "first", "20240101000000"), syPara(p2, "second changed", "20240102000000"))
	relPath := "/" + filepath.Base(filepath.Dir(tempDir)) + ".sy"
	putVersion := func(data []byte, updated int64) *entity.File {
		chunkID := util.Hash(data)
		if err = repo.store.PutChunk(&entity.Chunk{ID: chunkID, Data: data}); nil != err {
			t.Fatal(err)
		}
		file := entity.NewFile(relPath, int64(len(data)), updated)
		file.Chunks = []string{chunkID}
		if err = repo.store.PutFile(file); nil != err {
			t.Fatal(err)
		}
		return file
	}
	baseFile := putVersion(base, time.Now().Add(-3*time.Second).UnixMilli())
	localFile := putVersion(local, time.Now().Add(-2*time.Second).UnixMilli())
	cloudUpdated := time.Now().Add(-time.Second).UnixMilli()
	// 避免故障注入目录碰巧包含已有的输入版本对象。
	var cloud []byte
	var chunkDir string
	for i := 0; i < 1024; i++ {
		cloud = syDoc(syPara(p1, "first", "20240101000000"), syPara(p2, "second", "20240101000000"), syPara(p3, fmt.Sprintf("cloud %d", i), "20240103000000"))
		mergedData := mustMerge(t, base, local, cloud)
		chunkDir, _ = repo.store.AbsPath(util.Hash(mergedData))
		cloudChunkDir, _ := repo.store.AbsPath(util.Hash(cloud))
		cloudFileDir, _ := repo.store.AbsPath(entity.NewFile(relPath, int64(len(cloud)), cloudUpdated).ID)
		if _, statErr := os.Stat(chunkDir); errors.Is(statErr, os.ErrNotExist) && chunkDir != cloudChunkDir && chunkDir != cloudFileDir {
			break
		}
		chunkDir = ""
	}
	if "" == chunkDir {
		t.Fatal("could not find an unused chunk directory")
	}
	cloudFile := putVersion(cloud, cloudUpdated)

	absPath := filepath.Join(dataPath, relPath)
	if err = os.WriteFile(absPath, local, 0644); nil != err {
		t.Fatal(err)
	}
	localTime := time.UnixMilli(localFile.Updated)
	if err = os.Chtimes(absPath, localTime, localTime); nil != err {
		t.Fatal(err)
	}

	// 让合并结果的分块无法入库。把分块目录位置成普通文件，
	// 比目录权限更稳定，也不依赖测试进程是否以 root 运行。
	if err = os.MkdirAll(filepath.Dir(chunkDir), 0755); nil != err {
		t.Fatal(err)
	}
	if err = os.WriteFile(chunkDir, []byte("blocking file"), 0644); nil != err {
		t.Fatal(err)
	}
	defer os.Remove(chunkDir)

	mergedFile, err := repo.mergeStructuredSyncFile(baseFile, localFile, cloudFile, "20240101000000-000000", map[string]interface{}{})
	if nil == err {
		t.Fatalf("expected the indexing failure to be propagated, got merged file [%v]", mergedFile)
	}
	if nil != mergedFile {
		t.Fatalf("no merged file must be returned on failure, got [%s]", mergedFile.ID)
	}

	// 数据目录和时间戳始终不变。
	data, err := os.ReadFile(absPath)
	if nil != err {
		t.Fatal(err)
	}
	if string(data) != string(local) {
		t.Fatalf("data directory changed:\n%s", data)
	}
	info, err := os.Stat(absPath)
	if nil != err || info.ModTime().UnixMilli() != localFile.Updated {
		t.Fatalf("local timestamp changed: %v (%v)", info, err)
	}
}

// 按需下载同步入库失败时必须中止；清除故障后重试能上传、下载完整合并结果。
func TestAssetSyncStructuredMergeIndexFailure(t *testing.T) {
	baseDir := t.TempDir()
	remote := filepath.Join(baseDir, "cloud")
	full := newAssetTestRepo(t, baseDir, remote, "full", false)
	partial := newAssetTestRepo(t, baseDir, remote, "partial", true)
	path := "/" + filepath.Base(filepath.Dir(baseDir)) + ".sy"
	base := syDoc(syPara(p1, "first", "20240101000000"), syPara(p2, "second", "20240101000000"))
	local := syDoc(syPara(p1, "first", "20240101000000"), syPara(p2, "local", "20240102000000"))
	writeAssetTestFile(t, full, path, string(base), 10)
	syncAssetTestRepo(t, full)
	syncAssetTestRepo(t, partial)
	writeAssetTestFile(t, partial, path, string(local), 12)
	if _, err := partial.Index("local", true, nil); err != nil {
		t.Fatal(err)
	}
	localSnapshot := assetTestFile(t, partial, path)

	var cloud, merged []byte
	var chunkDir string
	cloudUpdated := time.Unix(1700000013, 0).UnixMilli()
	for i := 0; i < 1024; i++ {
		cloud = syDoc(syPara(p1, fmt.Sprintf("cloud %d", i), "20240103000000"), syPara(p2, "second", "20240101000000"))
		merged = mustMerge(t, base, local, cloud)
		chunkDir, _ = partial.store.AbsPath(util.Hash(merged))
		cloudChunkDir, _ := partial.store.AbsPath(util.Hash(cloud))
		cloudFileDir, _ := partial.store.AbsPath(entity.NewFile(path, int64(len(cloud)), cloudUpdated).ID)
		if _, statErr := os.Stat(chunkDir); errors.Is(statErr, os.ErrNotExist) && chunkDir != cloudChunkDir && chunkDir != cloudFileDir {
			break
		}
		chunkDir = ""
	}
	if chunkDir == "" {
		t.Fatal("could not find an unused chunk directory")
	}
	writeAssetTestFile(t, full, path, string(cloud), 13)
	syncAssetTestRepo(t, full)
	cloudBefore, err := full.GetCloudLatest(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(chunkDir, []byte("blocking file"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, _, err = partial.Sync(nil); err == nil || !strings.Contains(err.Error(), "index structured merge result") {
		t.Fatalf("expected merge indexing error, got %v", err)
	}
	got, err := os.ReadFile(partial.absPath(path))
	if err != nil || !bytes.Equal(got, local) {
		t.Fatalf("local data not restored: %q (%v)", got, err)
	}
	info, err := os.Stat(partial.absPath(path))
	if err != nil || info.ModTime().UnixMilli() != localSnapshot.Updated {
		t.Fatalf("local timestamp not restored: %v (%v)", info, err)
	}
	cloudAfter, err := full.GetCloudLatest(nil)
	if err != nil || cloudAfter.ID != cloudBefore.ID {
		t.Fatalf("failed sync advanced cloud: %v (%v)", cloudAfter, err)
	}
	if err = os.Remove(chunkDir); err != nil {
		t.Fatal(err)
	}
	if _, _, err = partial.Sync(nil); err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	got, err = os.ReadFile(partial.absPath(path))
	if err != nil || !bytes.Equal(got, merged) {
		t.Fatalf("retry did not write merged data: %q (%v)", got, err)
	}
	syncAssetTestRepo(t, full)
	got, err = os.ReadFile(full.absPath(path))
	if err != nil || !bytes.Equal(got, merged) {
		t.Fatalf("other device did not receive merged data: %q (%v)", got, err)
	}
}

func TestStructuredMergeRespectsCheckoutPreflight(t *testing.T) {
	for _, onDemand := range []bool{false, true} {
		t.Run(fmt.Sprintf("on-demand-%v", onDemand), func(t *testing.T) {
			baseDir := t.TempDir()
			remote := filepath.Join(baseDir, "cloud")
			source := newAssetTestRepo(t, baseDir, remote, "source", false)
			target := newAssetTestRepo(t, baseDir, remote, "target", onDemand)
			if !onDemand {
				target.assetDownloads = nil
			}
			path := "/" + filepath.Base(filepath.Dir(baseDir)) + ".sy"
			base := syDoc(syPara(p1, "first", "20240101000000"), syPara(p2, "second", "20240101000000"))
			local := syDoc(syPara(p1, "first", "20240101000000"), syPara(p2, "local", "20240102000000"))
			cloud := syDoc(syPara(p1, "cloud", "20240103000000"), syPara(p2, "second", "20240101000000"))
			merged := mustMerge(t, base, local, cloud)
			writeAssetTestFile(t, source, path, string(base), 10)
			syncAssetTestRepo(t, source)
			syncAssetTestRepo(t, target)
			writeAssetTestFile(t, target, path, string(local), 12)
			if _, err := target.Index("local", true, nil); err != nil {
				t.Fatal(err)
			}
			writeAssetTestFile(t, source, path, string(cloud), 13)
			syncAssetTestRepo(t, source)

			rejected := errors.New("preflight rejected")
			called := false
			target.SetCheckoutHooks(CheckoutHooks{Preflight: func(plan *CheckoutPlan) error {
				called = true
				if data, err := plan.ReadFile(path); err != nil || !bytes.Equal(data, merged) {
					t.Errorf("preflight received incorrect merged contents: %v", err)
				}
				if data, err := os.ReadFile(target.absPath(path)); err != nil || !bytes.Equal(data, local) {
					t.Errorf("local file changed before preflight: %v", err)
				}
				return rejected
			}})
			if _, _, err := target.Sync(nil); !errors.Is(err, rejected) {
				t.Fatalf("preflight rejection not returned: %v", err)
			}
			if !called {
				t.Fatal("preflight was not called")
			}
			if data, err := os.ReadFile(target.absPath(path)); err != nil || !bytes.Equal(data, local) {
				t.Fatalf("rejected merge changed local file: %v", err)
			}
		})
	}
}

func TestStructuredMergeIncludesEditSavedDuringCheckoutPreparation(t *testing.T) {
	baseDir := t.TempDir()
	remote := filepath.Join(baseDir, "cloud")
	source := newAssetTestRepo(t, baseDir, remote, "source", false)
	target := newAssetTestRepo(t, baseDir, remote, "target", true)
	path := "/" + filepath.Base(filepath.Dir(baseDir)) + ".sy"
	base := syDoc(syPara(p1, "first", "20240101000000"), syPara(p2, "second", "20240101000000"))
	local := syDoc(syPara(p1, "first", "20240101000000"), syPara(p2, "local", "20240102000000"))
	late := syDoc(syPara(p1, "first", "20240101000000"), syPara(p2, "saved during preparation", "20240104000000"))
	cloud := syDoc(syPara(p1, "cloud", "20240103000000"), syPara(p2, "second", "20240101000000"))
	writeAssetTestFile(t, source, path, string(base), 10)
	syncAssetTestRepo(t, source)
	syncAssetTestRepo(t, target)
	writeAssetTestFile(t, target, path, string(local), 12)
	if _, err := target.Index("local", true, nil); err != nil {
		t.Fatal(err)
	}
	writeAssetTestFile(t, source, path, string(cloud), 13)
	syncAssetTestRepo(t, source)
	prepared := false
	target.SetCheckoutHooks(CheckoutHooks{Apply: func() error {
		if !prepared {
			prepared = true
			if data, err := os.ReadFile(target.absPath(path)); err != nil || !bytes.Equal(data, local) {
				t.Errorf("merge was written before saving edits: %v", err)
			}
			writeAssetTestFile(t, target, path, string(late), 14)
		}
		return nil
	}})
	if _, _, err := target.Sync(nil); err != nil {
		t.Fatal(err)
	}
	if !prepared {
		t.Fatal("checkout preparation was not called")
	}
	want := mustMerge(t, base, late, cloud)
	if got, err := os.ReadFile(target.absPath(path)); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("saved edit and cloud changes not merged: %v", err)
	}
}
