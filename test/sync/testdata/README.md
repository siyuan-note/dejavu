# Sync Scenario Cases

`TestSyncScenariosFromJSON` loads cases from `test/sync/testdata/cases`.
It supports both layouts:

```text
test/sync/testdata/cases/basic/config.json
test/sync/testdata/cases/one-off.json
```

Current grading:

- `basic`: core sync behavior that should remain stable.
- `edge`: current edge behavior and multi-client convergence scenarios.
- `known-conflicts`: current conflict-producing behavior that is expected today. When a fix changes the behavior, update the expectation and move the case to `basic` or `edge`.
- `sync-download`: manual download-only sync behavior, which uses a different code path from `sync`.

Each `config.json` or top-level `*.json` file can contain one case object or an
array of case objects.

Each case starts from one seeded and synced client. Every named client is cloned
from that synced baseline, so cases model multiple devices that share the same
last sync point.

## Case Format

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

## Fields

- `name`: test name.
- `skip`: optional skip reason for blueprint scenarios that should not run yet.
- `seed`: initial files, keyed by relative path.
- `seedDir`: optional fixture directory copied into the seeded data directory.
- `clients`: device names cloned from the seeded synced baseline.
- `steps`: ordered operations to run.
- `final`: optional final-state assertions keyed by client name.

## Step Ops

- `write`: writes `content` to `path`. If `source` is set, reads content from a fixture file relative to the case directory. `minutes` sets the file mtime relative to the fixed scenario base time.
- `apply_dir`: copies `sourceDir` from the case directory into the client data directory and updates copied file mtimes.
- `remove`: removes `path`.
- `index`: creates a local snapshot. `memo` is optional.
- `sync`: runs cloud sync. Optional `want` asserts merge result counts.
- `sync_download`: runs download-only cloud sync. Optional `want` asserts merge result counts.
- `assert`: checks that `path` has exact `content`.
- `assert_missing`: checks that `path` does not exist.

`want` supports:

```json
{"upserts": 0, "removes": 0, "conflicts": 0}
```

`final` supports:

```json
{
  "a": {
    "files": {"doc.txt": "content\n"},
    "sources": {"storage/sync-demo.json": "fixtures/a/storage/sync-demo.json"},
    "missing": ["deleted.txt"]
  }
}
```
