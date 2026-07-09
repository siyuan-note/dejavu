# DejaVu

[中文](README.zh-CN.md)

## 💡 Introduction

[DejaVu](https://github.com/siyuan-note/dejavu) is the component of data snapshot and sync for [SiYuan](https://github.com/siyuan-note/siyuan).

## ✨ Features

* Git-like version control
* File deduplication in chunks
* Data compression
* AES Encrypted
* Cloud sync and backup

⚠️ Attention

* Folders are not supported
* Permission attributes are not supported
* Symbolic links are not supported

## 🎨 Design

Design reference from [ArtiVC](https://github.com/InfuseAI/ArtiVC).

### Entity

* `ID` Each entity is identified by SHA-1
* `Index` file list, each index operation generates a new index
    * `memo` index memo
    * `created` index time
    * `files` file list
    * `count` count of total files
    * `size` size of total files
* `File` file, a new file is generated when the actual data file path or content changes
    * `path` file path
    * `size` file size
    * `updated` last update time
    * `chunks` file chunk list
* `Chunk` file chunk
    * `data` actual data
* `Ref` refers to the index
    * `latest` built-in reference, automatically points to the latest index
    * `tag` tag reference, manually point to the specified index
* `Repo` repository

### Repo

* `DataPath` data folder path, the folder where the actual data file is located
* `Path` repo folder path, the repo is not stored in the data folder, we need to specify the repo folder path separately

The repo folder layout is as follows:

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

## 📄 License

DejaVu uses the [GNU AFFERO GENERAL PUBLIC LICENSE, Version 3](https://www.gnu.org/licenses/agpl-3.0.txt) open source license.

## 🙏 Acknowledgement

* [https://github.com/dustin/go-humanize](https://github.com/dustin/go-humanize) `MIT license`
* [https://github.com/klauspost/compress](https://github.com/klauspost/compress) `BSD-3-Clause license`
* [https://github.com/panjf2000/ants](https://github.com/panjf2000/ants) `MIT license`
* [https://github.com/InfuseAI/ArtiVC](https://github.com/InfuseAI/ArtiVC) `Apache-2.0 license`
* [https://github.com/restic/restic](https://github.com/restic/restic) `BSD-2-Clause license`
* [https://github.com/sabhiram/go-gitignore](https://github.com/sabhiram/go-gitignore) `MIT license`
