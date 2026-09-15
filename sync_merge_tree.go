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
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/88250/lute"
	"github.com/88250/lute/ast"
	"github.com/88250/lute/parse"
	"github.com/88250/lute/render"
	"github.com/siyuan-note/dataparser"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/filelock"
	"github.com/siyuan-note/logging"
)

// 结构化三方合并：思源文档 (.sy) 不是按文本行编辑的，所以不能像 git 一样按行合并，
// 这里以块 ID 为键在语法树上做三方合并。规则和 git 的三方合并一致，只是最小单位是块：
//
//   - 只有一端相对上次同步点改过的块，直接采用改过的一端；
//   - 两端改成一样的块，采用任意一端；
//   - 两端改得不一样、或者一端删除另一端修改，视为冲突，整个文件回退到现有的文件级冲突处理，绝不猜测。
//
// 容器块（文档、列表、列表项、引述、超级块等）先合并自身属性，再对子块 ID 序列做 diff3 决定顺序和增删；
// 叶子块按整棵子树的内容签名比较，签名忽略 updated 属性，避免仅刷新时间戳的块被当成修改。

// syNodeSignatureIgnoredAttrs 是计算块签名时忽略的属性：只反映编辑时间或运行态，不代表内容变化。
var syNodeSignatureIgnoredAttrs = []string{"updated", "refcount", "av-names"}

// mergeSyDocuments 对同一 .sy 文档的上次同步版本、本地版本和云端版本做结构化三方合并。
// 返回 ok=false 表示存在无法自动合并的修改，调用方应回退到文件级冲突处理；err 仅表示解析或渲染失败。
func mergeSyDocuments(base, local, cloud []byte, luteEngine *lute.Lute) (merged []byte, ok bool, err error) {
	baseTree, err := dataparser.ParseJSONWithoutFix(base, luteEngine.ParseOptions)
	if nil != err {
		return
	}
	localTree, err := dataparser.ParseJSONWithoutFix(local, luteEngine.ParseOptions)
	if nil != err {
		return
	}
	cloudTree, err := dataparser.ParseJSONWithoutFix(cloud, luteEngine.ParseOptions)
	if nil != err {
		return
	}

	mergedRoot, ok := mergeSyNode(baseTree.Root, localTree.Root, cloudTree.Root)
	if !ok {
		return
	}

	// 和思源内核落盘 .sy 的方式保持一致：JSON 渲染后按制表符缩进
	tree := &parse.Tree{Name: localTree.Name, ID: mergedRoot.ID, Root: mergedRoot, Context: &parse.Context{ParseOption: luteEngine.ParseOptions}}
	renderer := render.NewJSONRenderer(tree, luteEngine.RenderOptions, luteEngine.ParseOptions)
	data := renderer.Render()
	buf := bytes.Buffer{}
	if err = json.Indent(&buf, data, "", "\t"); nil != err {
		ok = false
		return
	}
	merged = buf.Bytes()

	// 回读校验合并结果，避免把无法解析的文档写回数据目录
	if _, err = dataparser.ParseJSONWithoutFix(merged, luteEngine.ParseOptions); nil != err {
		merged, ok = nil, false
		return
	}
	return
}

// mergeSyNode 合并同一个块的三个版本，local 和 cloud 必须非空，base 为空表示两端各自新增了同一个块。
func mergeSyNode(base, local, cloud *ast.Node) (ret *ast.Node, ok bool) {
	if nil == base {
		if syFullSignature(local) == syFullSignature(cloud) {
			return syDetach(local), true
		}
		return nil, false
	}

	if base.Type != local.Type || base.Type != cloud.Type || !base.IsContainerBlock() {
		return mergeSyLeaf(base, local, cloud)
	}

	own, ok := pickSyNode(syOwnSignature(base), syOwnSignature(local), syOwnSignature(cloud), local, cloud)
	if !ok {
		return nil, false
	}
	children, ok := mergeSyChildren(base, local, cloud)
	if !ok {
		return nil, false
	}

	ret = syShallowCopy(own)
	for _, child := range children {
		ret.AppendChild(child)
	}
	return ret, true
}

// mergeSyLeaf 按整棵子树的签名合并非容器块。
func mergeSyLeaf(base, local, cloud *ast.Node) (*ast.Node, bool) {
	ret, ok := pickSyNode(syFullSignature(base), syFullSignature(local), syFullSignature(cloud), local, cloud)
	if !ok {
		return nil, false
	}
	return syDetach(ret), true
}

// pickSyNode 根据三个版本的签名选出应采用的一端。
func pickSyNode(baseSig, localSig, cloudSig string, local, cloud *ast.Node) (*ast.Node, bool) {
	switch {
	case localSig == cloudSig:
		return local, true
	case localSig == baseSig:
		return cloud, true
	case cloudSig == baseSig:
		return local, true
	}
	return nil, false
}

// mergeSyChildren 对容器块的子块序列做三方合并。
func mergeSyChildren(base, local, cloud *ast.Node) (ret []*ast.Node, ok bool) {
	baseKeys, baseByKey := syChildKeys(base)
	localKeys, localByKey := syChildKeys(local)
	cloudKeys, cloudByKey := syChildKeys(cloud)

	order, ok := diff3Sequence(baseKeys, localKeys, cloudKeys)
	if !ok {
		return nil, false
	}
	kept := make(map[string]bool, len(order))
	for _, key := range order {
		kept[key] = true
	}

	// 一端删除的块，另一端不能有修改，否则这个修改会被静默丢弃
	for key, baseNode := range baseByKey {
		if kept[key] {
			continue
		}
		if localNode := localByKey[key]; nil != localNode && syFullSignature(localNode) != syFullSignature(baseNode) {
			return nil, false
		}
		if cloudNode := cloudByKey[key]; nil != cloudNode && syFullSignature(cloudNode) != syFullSignature(baseNode) {
			return nil, false
		}
	}

	ret = make([]*ast.Node, 0, len(order))
	for _, key := range order {
		baseNode, localNode, cloudNode := baseByKey[key], localByKey[key], cloudByKey[key]
		switch {
		case nil != localNode && nil != cloudNode:
			merged, mergedOK := mergeSyNode(baseNode, localNode, cloudNode)
			if !mergedOK {
				return nil, false
			}
			ret = append(ret, merged)
		case nil != localNode:
			if nil != baseNode {
				return nil, false // 云端删除了本地仍保留的块，而顺序合并却保留了它，属于无法判断的情况
			}
			ret = append(ret, syDetach(localNode))
		case nil != cloudNode:
			if nil != baseNode {
				return nil, false
			}
			ret = append(ret, syDetach(cloudNode))
		default:
			return nil, false
		}
	}
	return ret, true
}

// syChildKeys 返回容器块的子节点键序列。块节点用 ID，没有 ID 的标记节点（引述、超级块的标记等）用类型加内容并按出现次序编号。
func syChildKeys(n *ast.Node) (keys []string, byKey map[string]*ast.Node) {
	byKey = map[string]*ast.Node{}
	occurrence := map[string]int{}
	for child := n.FirstChild; nil != child; child = child.Next {
		key := child.ID
		if "" == key {
			key = "\x00" + child.Type.String() + "\x00" + string(child.Tokens)
		}
		if count := occurrence[key]; 0 < count || "" == child.ID {
			occurrence[key] = count + 1
			key += "#" + strconv.Itoa(count)
		} else {
			occurrence[key] = 1
		}
		keys = append(keys, key)
		byKey[key] = child
	}
	return
}

// syOwnSignature 返回节点自身（不含子节点）的内容签名，序列化方式和 .sy 落盘一致。
func syOwnSignature(n *ast.Node) string {
	c := *n
	c.Parent, c.Previous, c.Next, c.FirstChild, c.LastChild, c.Children = nil, nil, nil, nil, nil, nil
	c.Data, c.TypeStr = string(n.Tokens), n.Type.String()
	c.Properties = map[string]string{}
	for _, kv := range n.KramdownIAL {
		c.Properties[kv[0]] = kv[1]
	}
	for _, attr := range syNodeSignatureIgnoredAttrs {
		delete(c.Properties, attr)
	}
	data, err := json.Marshal(&c)
	if nil != err {
		// 无法序列化时退化为按类型和内容比较
		return c.TypeStr + "\x00" + c.Data
	}
	return string(data)
}

// syFullSignature 返回节点整棵子树的内容签名。
func syFullSignature(n *ast.Node) string {
	builder := strings.Builder{}
	syWriteFullSignature(&builder, n)
	return builder.String()
}

func syWriteFullSignature(builder *strings.Builder, n *ast.Node) {
	builder.WriteString(syOwnSignature(n))
	if nil == n.FirstChild {
		return
	}
	builder.WriteByte('[')
	for child := n.FirstChild; nil != child; child = child.Next {
		syWriteFullSignature(builder, child)
		builder.WriteByte(',')
	}
	builder.WriteByte(']')
}

// syShallowCopy 复制节点自身属性，不带任何树上的链接。
func syShallowCopy(n *ast.Node) *ast.Node {
	c := *n
	c.Parent, c.Previous, c.Next, c.FirstChild, c.LastChild, c.Children = nil, nil, nil, nil, nil, nil
	return &c
}

// syDetach 把节点从原来的树上摘下来，供合并结果复用整棵子树。
func syDetach(n *ast.Node) *ast.Node {
	n.Unlink()
	return n
}

// diff3Sequence 对三个键序列做 diff3 合并。键在每个序列内唯一。
// 以 base 中同时出现在 local 和 cloud 里的元素为稳定锚点，锚点之间的区段按三方规则取舍，两端改得不一样则返回 ok=false。
func diff3Sequence(base, local, cloud []string) (ret []string, ok bool) {
	localMatch := uniqueLCSMatch(base, local)
	cloudMatch := uniqueLCSMatch(base, cloud)

	bi, li, ci := 0, 0, 0
	for {
		j := bi
		for j < len(base) {
			_, inLocal := localMatch[j]
			_, inCloud := cloudMatch[j]
			if inLocal && inCloud {
				break
			}
			j++
		}

		lEnd, cEnd := len(local), len(cloud)
		if j < len(base) {
			lEnd, cEnd = localMatch[j], cloudMatch[j]
		}
		baseSeg, localSeg, cloudSeg := base[bi:j], local[li:lEnd], cloud[ci:cEnd]
		switch {
		case equalStrings(localSeg, baseSeg):
			ret = append(ret, cloudSeg...)
		case equalStrings(cloudSeg, baseSeg):
			ret = append(ret, localSeg...)
		case equalStrings(localSeg, cloudSeg):
			ret = append(ret, localSeg...)
		default:
			return nil, false
		}

		if j >= len(base) {
			return ret, true
		}
		ret = append(ret, base[j])
		bi, li, ci = j+1, lEnd+1, cEnd+1
	}
}

// uniqueLCSMatch 计算两个元素唯一的序列的最长公共子序列，返回 a 的下标到 b 的下标的匹配。
func uniqueLCSMatch(a, b []string) map[int]int {
	posInB := make(map[string]int, len(b))
	for i, key := range b {
		posInB[key] = i
	}

	type pair struct{ ai, bi int }
	var pairs []pair
	for i, key := range a {
		if j, found := posInB[key]; found {
			pairs = append(pairs, pair{i, j})
		}
	}

	// 对 b 下标序列求最长递增子序列（耐心排序）
	tails := make([]int, 0, len(pairs)) // tails[k] 为长度 k+1 的递增子序列的最小结尾在 pairs 中的下标
	prev := make([]int, len(pairs))     // 前驱
	for idx, p := range pairs {
		lo, hi := 0, len(tails)
		for lo < hi {
			mid := (lo + hi) / 2
			if pairs[tails[mid]].bi < p.bi {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		if 0 < lo {
			prev[idx] = tails[lo-1]
		} else {
			prev[idx] = -1
		}
		if lo == len(tails) {
			tails = append(tails, idx)
		} else {
			tails[lo] = idx
		}
	}

	ret := make(map[int]int, len(tails))
	if 0 == len(tails) {
		return ret
	}
	for idx := tails[len(tails)-1]; -1 != idx; idx = prev[idx] {
		ret[pairs[idx].ai] = pairs[idx].bi
	}
	return ret
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

// mergeStructuredSyncFile 尝试对同一 .sy 文件的本地修改和云端修改做结构化合并。
// 合并成功时把结果写入数据目录并返回 true，随后的合并索引会把它作为本地变更纳入；失败时返回 false，由调用方按文件级冲突处理。
func (repo *Repo) mergeStructuredSyncFile(base, local, cloud *entity.File, now string, context map[string]interface{}) bool {
	if nil == base || nil == local || nil == cloud || !strings.HasSuffix(local.Path, ".sy") {
		return false
	}

	temp := filepath.Join(repo.TempPath, "repo", "sync", "merges", now, strings.TrimPrefix(local.Path, "/"))
	defer os.RemoveAll(temp)

	baseData, err := repo.readFileVersion(base, filepath.Join(temp, "base"), context)
	if nil != err {
		return false
	}
	localData, err := repo.readFileVersion(local, filepath.Join(temp, "local"), context)
	if nil != err {
		return false
	}
	cloudData, err := repo.readFileVersion(cloud, filepath.Join(temp, "cloud"), context)
	if nil != err {
		return false
	}

	merged, ok, err := mergeSyDocuments(baseData, localData, cloudData, lute.New())
	if nil != err {
		logging.LogWarnf("structured merge [%s] failed: %s", local.Path, err)
		return false
	}
	if !ok {
		logging.LogInfof("structured merge [%s] found conflicting block changes, fallback to file conflict", local.Path)
		return false
	}

	if err = filelock.WriteFile(repo.absPath(local.Path), merged); nil != err {
		logging.LogErrorf("write structured merge result [%s] failed: %s", local.Path, err)
		return false
	}
	logging.LogInfof("structured merged [%s]", local.Path)
	return true
}

// readFileVersion 把仓库中的某个文件版本检出到临时目录并读取内容。
func (repo *Repo) readFileVersion(file *entity.File, checkoutDir string, context map[string]interface{}) (data []byte, err error) {
	if repo.assetDownloads != nil {
		if err = repo.ensureFileChunks(file, context); nil != err {
			return
		}
	}
	checkoutTmp, err := repo.store.GetFile(file.ID)
	if nil != err {
		logging.LogErrorf("get file failed: %s", err)
		return
	}
	if err = repo.checkoutFile(checkoutTmp, checkoutDir, 1, 1, context); nil != err {
		logging.LogErrorf("checkout file failed: %s", err)
		return
	}
	data, err = os.ReadFile(filepath.Join(checkoutDir, checkoutTmp.Path))
	if nil != err {
		logging.LogErrorf("read file failed: %s", err)
	}
	return
}
