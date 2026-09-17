# 固定旧版兼容驱动

此独立 Go module 固定依赖已发布的 DejaVu `v0.0.0-20260914113714-464364dd8fad`，不使用当前仓库的 replace，也不复制或修改旧同步引擎源码。在 DejaVu 根目录运行 `python scripts/test-appearance-compat.py`，脚本会校验实际解析的模块版本且没有 replace，并运行 `go mod verify` 检查模块缓存未被修改，再分别调用旧驱动及当前源码的临时冻结副本。默认覆盖完整下载／按需下载与双向同步／手动上传下载四种组合，也可用 `--case full-manual` 单独运行一种组合。

驱动通过 `go run -mod=readonly . -config <临时 JSON 配置文件>` 调用。配置、工作区、数据仓库、历史和 Local cloud 均限制在系统临时目录；默认密钥是仅用于测试的 ASCII `0123456789abcdef0123456789abcdef`。支持 Index、Sync、单向上传／下载、Checkout、标签备份／下载、本地清理、云端清理及认证读取后的文件摘要输出。每次请求和响应由上层脚本保存，便于复查。

原始旧 `cloud.Local.ListObjects("refs/")` 会把 `tags` 目录当作对象，导致含任何普通标签的 `PurgeCloud` 尝试读取目录并失败。驱动默认保留这个行为；仅在配置 `recursiveRefs: true` 时用测试适配器补齐引用递归枚举的后端契约。适配器列出全部嵌套引用，不特殊处理外观标签，不改固定旧引擎的清理、同步、文件标识或认证代码。混合测试先保存原始 Local 失败，再使用该适配器验证旧清理算法能保留普通标签引用的完整恢复材料；这不等同于官方云服务的端到端验证。
