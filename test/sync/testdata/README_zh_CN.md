# 同步场景测试用例

`TestSyncScenariosFromJSON` 会从 `test/sync/testdata/cases` 加载测试用例。
待修复清单见 `test/TODO_zh_CN.md`。
支持两种布局：

```text
test/sync/testdata/cases/basic/config.json
test/sync/testdata/cases/one-off.json
```

当前分级：

- `basic`：核心同步行为，应该长期保持稳定。
- `edge`：当前边界行为、多客户端最终一致性场景。
- `known-conflicts`：当前就是会产生冲突的行为。修复后更新期望，并移动到 `basic` 或 `edge`。
- `sync-download`：手动“仅下载”同步行为，它和 `sync` 走不同代码链路。

每个 `config.json` 或顶层 `*.json` 可以是单个 case 对象，也可以是 case 数组。

每个 case 都会先创建一个已完成同步的 seed 客户端。`clients` 中声明的每台设备都会从这个同步基线克隆出来，所以能模拟“多台设备拥有同一个上次同步点”的状态。

## 用例格式

```json
{
  "name": "remote update applies without conflict",
  "seed": {
    "doc.txt": "base\n"
  },
  "clients": ["a", "b"],
  "steps": [
    {"client": "a", "op": "write", "path": "doc.txt", "content": "from a\n", "minutes": 10},
    {"client": "a", "op": "index", "memo": "a update"},
    {"client": "a", "op": "sync", "want": {"upserts": 0, "removes": 0, "conflicts": 0}},
    {"client": "b", "op": "sync", "want": {"upserts": 1, "removes": 0, "conflicts": 0}},
    {"client": "b", "op": "assert", "path": "doc.txt", "content": "from a\n"}
  ],
  "final": {
    "a": {"files": {"doc.txt": "from a\n"}},
    "b": {"files": {"doc.txt": "from a\n"}}
  }
}
```

## 字段

- `name`：测试名称。
- `skip`：可选，跳过原因。用于记录暂时不运行的蓝图场景，比如需要产品合并策略的同字段编辑。
- `seed`：初始文件，key 是相对路径，value 是文件内容。
- `seedDir`：可选，fixture 目录，会复制到初始 data 目录。
- `clients`：设备名列表，每台设备都会从已同步基线克隆。
- `steps`：按顺序执行的操作。
- `final`：可选，按设备声明最终状态断言。

## Step 操作

- `write`：写入 `content` 到 `path`。如果设置了 `source`，则从当前 case 目录下的 fixture 文件读取内容。`minutes` 用于设置相对固定基准时间的文件 mtime。
- `apply_dir`：把当前 case 目录下的 `sourceDir` 复制到客户端 data 目录，并更新复制文件的 mtime。
- `remove`：删除 `path`。
- `index`：创建本地快照。`memo` 可选。
- `sync`：执行云端同步。可用 `want` 断言 merge result 数量。
- `sync_download`：执行仅下载同步。可用 `want` 断言 merge result 数量。
- `assert`：断言 `path` 的内容等于 `content`。
- `assert_missing`：断言 `path` 不存在。

`want` 支持：

```json
{"upserts": 0, "removes": 0, "conflicts": 0}
```

`final` 支持：

```json
{
  "a": {
    "files": {"doc.txt": "content\n"},
    "sources": {"storage/sync-demo.json": "fixtures/a/storage/sync-demo.json"},
    "missing": ["deleted.txt"]
  }
}
```
