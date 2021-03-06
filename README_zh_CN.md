# DejaVu

[English](README.md)

## ð¡ ç®ä»

[DejaVu](https://github.com/siyuan-note/dejavu) æ¯ææºç¬è®°çæ°æ®å¿«ç§ååæ­¥ç»ä»¶ã

## â¨ ç¹æ§

* ç±»ä¼¼ Git ççæ¬æ§å¶
* æä»¶ååå»é
* æ°æ®åç¼©
* AES å å¯
* äºç«¯åæ­¥åå¤ä»½
* æéå è½½èµæºæä»¶

â ï¸ æ³¨æ

* ä¸æ¯ææä»¶å¤¹
* ä¸æ¯ææéå±æ§
* ä¸æ¯æç¬¦å·é¾æ¥

## ð¨ è®¾è®¡

è®¾è®¡åèèª [ArtiVC](https://github.com/InfuseAI/ArtiVC)ã

### å®ä½

* `ID` æ¯ä¸ªå®ä½é½éè¿ SHA-1 æ è¯
* `Index` æä»¶åè¡¨ï¼æ¯æ¬¡ç´¢å¼æä½é½çæä¸ä¸ªæ°çç´¢å¼
    * `parent` æåä¸ä¸ä¸ªç´¢å¼
    * `memo` ç´¢å¼å¤æ³¨
    * `created` ç´¢å¼æ¶é´
    * `files` æä»¶åè¡¨
    * `count` æä»¶æ»æ°
    * `size` æä»¶åè¡¨æ»å¤§å°
* `File` æä»¶ï¼å®éçæ°æ®æä»¶è·¯å¾æèåå®¹åçåå¨æ¶çæä¸ä¸ªæ°çæä»¶
    * `path` æä»¶è·¯å¾
    * `size` æä»¶å¤§å°
    * `updated` æåæ´æ°æ¶é´
    * `chunks` æä»¶åååè¡¨
* `Chunk` æä»¶å
    * `data` å®éçæ°æ®
* `Ref` å¼ç¨æåç´¢å¼
    * `latest` åç½®å¼ç¨ï¼èªå¨æåææ°çç´¢å¼
    * `tag` æ ç­¾å¼ç¨ï¼æå¨æåæå®çç´¢å¼
* `Repo` ä»åº

### ä»åº

* `DataPath` æ°æ®æä»¶å¤¹è·¯å¾ï¼å®éçæ°æ®æä»¶æå¨æä»¶å¤¹
* `Path` ä»åºæä»¶å¤¹è·¯å¾ï¼ä»åºä¸ä¿å­å¨æ°æ®æä»¶å¤¹ä¸­ï¼éè¦åç¬æå®ä»åºæä»¶å¤¹è·¯å¾

ä»åºæä»¶å¤¹ç»æå¦ä¸ï¼

```text
ââindexes
â      0531732dca85404e716abd6bb896319a41fa372b
â      19fc2c2e5317b86f9e048f8d8da2e4ed8300d8af
â      5f32d78d69e314beee36ad7de302b984da47ddd2
â      cbd254ca246498978d4f47e535bac87ad7640fe6
â
ââobjects
â  ââ1e
â  â      0ac5f319f5f24b3fe5bf63639e8dbc31a52e3b
â  â
â  ââ56
â  â      322ccdb61feab7f2f76f5eb82006bd51da7348
â  â
â  ââ7e
â  â      dccca8340ebe149b10660a079f34a20f35c4d4
â  â
â  ââ83
â  â      a7d72fe9a071b696fc81a3dc041cf36cbde802
â  â
â  ââ85
â  â      26b9a7efde615b67b4666ae509f9fbc91d370b
â  â
â  ââ87
â  â      1355acd062116d1713e8f7f55969dbb507a040
â  â
â  ââ96
â  â      46ba13a4e8eabeca4f5259bfd7da41d368a1a6
â  â
â  ââa5
â  â      5b8e6b9ccad3fc9b792d3d453a0793f8635b9f
â  â      b28787922f4e2a477b4f027e132aa7e35253d4
â  â
â  ââbe
â  â      c7a729d1b5f021f8eca0dd8b6ef689ad753567
â  â
â  ââd1
â  â      324c714bde18442b5629a84a361b5e7528b14a
â  â
â  ââf1
â  â      d7229171f4fa1c5eacb411995b16938a04f7f6
â  â
â  ââf7
â          ff9e8b7bb2e09b70935a5d785e0cc5d9d0abf0
â
âârefs
    â  latest
    â
    ââtags
            v1.0.0
            v1.0.1
```

## ð ææ

DejaVu ä½¿ç¨ [GNU Affero éç¨å¬å±è®¸å¯è¯, çæ¬ 3](https://www.gnu.org/licenses/agpl-3.0.txt) å¼æºåè®®ã

## ð é¸£è°¢

* [https://github.com/dustin/go-humanize](https://github.com/dustin/go-humanize) `MIT license`
* [https://github.com/klauspost/compress](https://github.com/klauspost/compress) `BSD-3-Clause license`
* [https://github.com/panjf2000/ants](https://github.com/panjf2000/ants) `MIT license`
* [https://github.com/InfuseAI/ArtiVC](https://github.com/InfuseAI/ArtiVC) `Apache-2.0 license`
* [https://github.com/restic/restic](https://github.com/restic/restic) `BSD-2-Clause license`
* [https://github.com/sabhiram/go-gitignore](https://github.com/sabhiram/go-gitignore) `MIT license`
