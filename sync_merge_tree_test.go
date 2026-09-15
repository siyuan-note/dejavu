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
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/88250/lute"
	"github.com/88250/lute/ast"
	"github.com/siyuan-note/dataparser"
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
