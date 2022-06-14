# DejaVu

[中文](README_zh_CN.md)

**Status: In development**

## 💡 Introduction

[DejaVu](https://github.com/siyuan-note/dejavu) is a golang library for data snapshot and synchronization.

## ✨ Features

* Git-like version control
* File deduplication in chunks
* Data compression
* AES Encrypted
* Connect to cloud storage

⚠️ Attention

* Folders are not supported
* Permission attributes are not supported
* Symbolic links are not supported

## 🎨 Design

Design reference from [ArtiVC](https://github.com/InfuseAI/ArtiVC).

### Entity

* `ID` Each entity is identified by SHA-1
* `Index` file list, each index operation generates a new index
    * `parent` points to the previous index
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
+---objects
|   +---00
|   |       e605c489491e553ef60eb13911cd1446ac6a0d
|   |
|   +---01
|   |       453ac7651a523eda839a0ef9b4d653f884c84a
|   |       cca40956df1159e8bbb56724cc80aca5fe378c
|   |       e58c0ae476b3fd79630e118e05527fc0a4ae54
|   |
|   +---03
|   |       0c904b32935936cafada2f54b6cfe3d02b2080
|   |       d61fb01c9abf0e6ec1279f98a3f1abfadcbfad
|   |       d6a0e2fba3b8d97539b9a54865d4e4f18b4a2f
|   |
|   +---08
|   |       8f46fa8bd3af4a32d17e80c93c849435d8e703
|   |
|   +---09
|   |       03cba26bd73c8849b750a07e19624f51df02ad
|   |       8fe907ab51c47082a83d0086d820aa1750c8a9
|   |
|   +---0a
|   |       7a7d148d34c87c344b1aa86edeef5242b5db6f
|   |       da61c49a77d61a7db9ef48def08b61311cff8b
|   \---ff
|           3d40c741e5a8491e578e93a5c20e054941ea07
|           41a69dd2283707cbd7baba1db6c8ce8116c9b5
|           ab8b9036fe7fabf3281d25de8c335cfbd77229
\---refs
    |   latest
    |
    \---tags
            v1.0.0
            v1.0.1
```

### Sync merge

TBD

## 📄 License

DejaVu uses the [Mulan Permissive Software License, Version 2](http://license.coscl.org.cn/MulanPSL2) open source
license.

## 🙏 Acknowledgement

* [ArtiVC](https://github.com/InfuseAI/ArtiVC)
* [restic](https://github.com/restic/restic)
