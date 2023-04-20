# DejaVu

[English](README.md)

## 💡 简介

[DejaVu](https://github.com/siyuan-note/dejavu) 是思源笔记的数据快照和同步组件。

## ✨ 特性

* 类似 Git 的版本控制
* 文件分块去重
* 数据压缩
* AES 加密
* 云端同步和备份

⚠️ 注意

* 不支持文件夹
* 不支持权限属性
* 不支持符号链接

## 🎨 设计

设计参考自 [ArtiVC](https://github.com/InfuseAI/ArtiVC)。

### 实体

* `ID` 每个实体都通过 SHA-1 标识
* `Index` 文件列表，每次索引操作都生成一个新的索引
    * `memo` 索引备注
    * `created` 索引时间
    * `files` 文件列表
    * `count` 文件总数
    * `size` 文件列表总大小
* `File` 文件，实际的数据文件路径或者内容发生变动时生成一个新的文件
    * `path` 文件路径
    * `size` 文件大小
    * `updated` 最后更新时间
    * `chunks` 文件分块列表
* `Chunk` 文件块
    * `data` 实际的数据
* `Ref` 引用指向索引
    * `latest` 内置引用，自动指向最新的索引
    * `tag` 标签引用，手动指向指定的索引
* `Repo` 仓库

### 仓库

* `DataPath` 数据文件夹路径，实际的数据文件所在文件夹
* `Path` 仓库文件夹路径，仓库不保存在数据文件夹中，需要单独指定仓库文件夹路径

仓库文件夹结构如下：

```text
├─indexes
│      0531732dca85404e716abd6bb896319a41fa372b
│      19fc2c2e5317b86f9e048f8d8da2e4ed8300d8af
│      5f32d78d69e314beee36ad7de302b984da47ddd2
│      cbd254ca246498978d4f47e535bac87ad7640fe6
│
├─objects
│  ├─1e
│  │      0ac5f319f5f24b3fe5bf63639e8dbc31a52e3b
│  │
│  ├─56
│  │      322ccdb61feab7f2f76f5eb82006bd51da7348
│  │
│  ├─7e
│  │      dccca8340ebe149b10660a079f34a20f35c4d4
│  │
│  ├─83
│  │      a7d72fe9a071b696fc81a3dc041cf36cbde802
│  │
│  ├─85
│  │      26b9a7efde615b67b4666ae509f9fbc91d370b
│  │
│  ├─87
│  │      1355acd062116d1713e8f7f55969dbb507a040
│  │
│  ├─96
│  │      46ba13a4e8eabeca4f5259bfd7da41d368a1a6
│  │
│  ├─a5
│  │      5b8e6b9ccad3fc9b792d3d453a0793f8635b9f
│  │      b28787922f4e2a477b4f027e132aa7e35253d4
│  │
│  ├─be
│  │      c7a729d1b5f021f8eca0dd8b6ef689ad753567
│  │
│  ├─d1
│  │      324c714bde18442b5629a84a361b5e7528b14a
│  │
│  ├─f1
│  │      d7229171f4fa1c5eacb411995b16938a04f7f6
│  │
│  └─f7
│          ff9e8b7bb2e09b70935a5d785e0cc5d9d0abf0
│
└─refs
    │  latest
    │
    └─tags
            v1.0.0
            v1.0.1
```

## 📄 授权

DejaVu 使用 [GNU Affero 通用公共许可证, 版本 3](https://www.gnu.org/licenses/agpl-3.0.txt) 开源协议。

## 🙏 鸣谢

* [https://github.com/dustin/go-humanize](https://github.com/dustin/go-humanize) `MIT license`
* [https://github.com/klauspost/compress](https://github.com/klauspost/compress) `BSD-3-Clause license`
* [https://github.com/panjf2000/ants](https://github.com/panjf2000/ants) `MIT license`
* [https://github.com/InfuseAI/ArtiVC](https://github.com/InfuseAI/ArtiVC) `Apache-2.0 license`
* [https://github.com/restic/restic](https://github.com/restic/restic) `BSD-2-Clause license`
* [https://github.com/sabhiram/go-gitignore](https://github.com/sabhiram/go-gitignore) `MIT license`
