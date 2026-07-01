# 测试修复 TODO

这个清单只记录“已有测试能描述、但当前实现还需要修复或决策”的同步问题。修完一项后，把对应测试从 `known-conflicts` 或 blueprint 状态移动到稳定分组，并更新期望。

## 必须修复

- [ ] 相同内容、不同 mtime 不应该产生冲突。
  - 当前测试：`test/sync/testdata/cases/known-conflicts/config.json`
  - Case：`same file same content but different timestamp reports conflict`
  - Case：`same path create same content but different timestamp reports conflict`
  - Case：`sync download same content but different timestamp reports conflict`
  - 当前期望：普通 `sync` 场景 `conflicts: 1`；`sync_download` 场景 `upserts: 1, conflicts: 1`
  - 修复目标：内容 hash 相同、路径相同、仅元数据时间不同的文件应视为已收敛，期望改为 `conflicts: 0`，然后移动到 `basic` 或 `edge`。

- [ ] 文本可合并的两端修改不应该在 `sync_download` 中直接文件级冲突。
  - 当前测试：`test/sync/testdata/cases/known-conflicts/config.json`
  - Case：`sync download remote blank line insert conflicts with local line edit`
  - 当前期望：`upserts: 1, conflicts: 1`，最终使用远端整文件内容。
  - 修复目标：如果后续支持文本 diff/patch，远端插入空行、本地修改另一行这类可合并修改应自动合并，期望改为 `conflicts: 0`。

## 后续可补充

- [ ] 增加“多文件事务”场景：同一次操作同时修改多个文件时，要么整体合并，要么整体冲突。
- [ ] 增加“字段可忽略”场景：确认某些纯缓存、更新时间、运行态字段是否应该参与冲突判断。
