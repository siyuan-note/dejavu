"""Run real pinned legacy/current appearance compatibility cases in temporary workspaces."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


REPO = Path(__file__).resolve().parents[1]
OLD_DRIVER = REPO / "testdata" / "appearance-legacy-client"
OLD_VERSION = "v0.0.0-20260914113714-464364dd8fad"
OLD_COMMIT = "464364dd8fadfdee29735024c30025cc65ed55aa"
NEW_IDENTITY = "new-appearance-working-tree-snapshot"
NOTE = "20260917000000-legacy1/20260917000001-oldnote.sy"
ASSET = "assets/appearance-compat.txt"
CASES = ("full-bidirectional", "ondemand-bidirectional", "full-manual", "ondemand-manual")
ENV = dict(os.environ, GOWORK="off", GOFLAGS="")


def go(arguments, cwd, timeout=180):
    result = subprocess.run(["go", *arguments], cwd=cwd, env=ENV, capture_output=True,
                            text=True, encoding="utf-8", timeout=timeout)
    if result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    return result.stdout


def prepare_new_driver(root):
    source = root / "new-dejavu"
    manifest = {}
    for original in [*REPO.rglob("*.go"), REPO / "go.mod", REPO / "go.sum"]:
        relative = original.relative_to(REPO)
        if ".git" in relative.parts or "testdata" in relative.parts:
            continue
        target = source / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        data = original.read_bytes()
        target.write_bytes(data)
        manifest[relative.as_posix()] = hashlib.sha256(data).hexdigest()
    (root / "new-source-manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    driver = root / "new-driver"
    driver.mkdir()
    code = (OLD_DRIVER / "main.go").read_text(encoding="utf-8")
    code = code.replace(f'const baseline = "{OLD_COMMIT}"', f'const baseline = "{NEW_IDENTITY}"')
    code = code.replace("type request struct {", 'type request struct {\nEnableAppearanceSync bool `json:"enableAppearanceSync"`')
    needle = "repo, err := dejavu.NewRepo(req.Data, req.Repo, req.History, req.Temp, req.Device, req.Device, runtime.GOOS, key, req.Ignore, local)"
    replacement = '''var repo *dejavu.Repo
    if req.EnableAppearanceSync {
        repo, err = dejavu.NewRepoWithOptions(dejavu.Options{
            DataPath: req.Data, RepoPath: req.Repo, HistoryPath: req.History, TempPath: req.Temp,
            DeviceID: req.Device, DeviceName: req.Device, DeviceOS: runtime.GOOS, AESKey: key,
            Cloud: local, IgnoreLines: req.Ignore, AppearanceIgnoreLines: req.Ignore,
            IgnoreRulePath: ".siyuan/syncignore", HiddenDirectoryNames: []string{".siyuan"},
            EnableAppearanceSync: true,
        })
    } else {
        repo, err = dejavu.NewRepo(req.Data, req.Repo, req.History, req.Temp, req.Device, req.Device, runtime.GOOS, key, req.Ignore, local)
    }'''
    if needle not in code:
        raise RuntimeError("legacy driver initialization changed; update the current-client adapter")
    (driver / "main.go").write_text(code.replace(needle, replacement), encoding="utf-8")
    module = (OLD_DRIVER / "go.mod").read_text(encoding="utf-8")
    (driver / "go.mod").write_text(module + "\nreplace github.com/siyuan-note/dejavu => ../new-dejavu\n", encoding="utf-8")
    shutil.copyfile(OLD_DRIVER / "go.sum", driver / "go.sum")
    go(["mod", "tidy"], driver)
    return driver


def write(p, data, timestamp):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)
    os.utime(p, (timestamp, timestamp))


def note(text, updated):
    return json.dumps({"ID": "20260917000001-oldnote", "Type": "NodeDocument",
        "Properties": {"id": "20260917000001-oldnote", "title": "Mixed compatibility", "updated": updated},
        "Children": [{"ID": "20260917000002-content", "Type": "NodeParagraph",
          "Properties": {"id": "20260917000002-content", "updated": updated},
          "Children": [{"Type": "NodeText", "Data": text}]}]}, separators=(",", ":")).encode()


def package_state(deleted):
    files = {} if deleted else {"theme.css": hashlib.sha256(b"body { color: black; }").hexdigest()}
    return json.dumps({"version": 1, "deleted": deleted, "migration": False, "files": files}, separators=(",", ":")).encode()


def run_case(root, new_driver, case, check_original_local):
    fixture = root / case
    fixture.mkdir()
    ondemand = case.startswith("ondemand")
    manual = case.endswith("manual")
    send = "syncUpload" if manual else "sync"
    receive = "syncDownload" if manual else "sync"

    def run(new, client, phase, actions, recursive_refs=False, expect_failure=False):
        base = fixture / client
        req = {"data": str(base / "data"), "repo": str(base / "repo"), "history": str(base / "history"),
               "temp": str(base / "temp"), "cloud": str(fixture / "cloud"), "device": client,
               "assetState": str(base / "asset-state.json"), "assetScope": "appearance-compat/" + case,
               "onDemand": ondemand, "actions": actions, "recursiveRefs": recursive_refs}
        if new:
            req["enableAppearanceSync"] = True
        request = fixture / (phase + ".request.json")
        request.write_text(json.dumps(req, indent=2), encoding="utf-8")
        result = subprocess.run(["go", "run", "-mod=readonly", ".", "-config", str(request)],
                                cwd=new_driver if new else OLD_DRIVER, env=ENV,
                                capture_output=True, text=True, encoding="utf-8", timeout=180)
        (fixture / (phase + ".response.json")).write_text(result.stdout, encoding="utf-8")
        if not result.stdout.strip():
            raise RuntimeError(phase + "\n" + result.stderr)
        parsed = json.loads(result.stdout)
        assert parsed["baseline"] == (NEW_IDENTITY if new else OLD_COMMIT)
        if expect_failure:
            assert result.returncode and "refs/tags" in parsed.get("error", "").replace("\\", "/"), parsed
        elif result.returncode:
            raise RuntimeError(phase + "\n" + result.stdout + result.stderr)
        print(case + "/" + phase + (" recorded original Local limitation" if expect_failure else " passed"), flush=True)
        return parsed

    initial = note("old note before appearance sync", "20260917000001")
    edited = note("old client can still edit this note", "20260917000003")
    after_checkout = note("old client edit after its early checkout", "20260917000005")
    for client in ("old", "new-a", "new-c"):
        write(fixture / client / "data/.siyuan/conf.json", b"{}", 1700000000)
        if client != "new-c":
            write(fixture / client / "data" / NOTE, initial, 1700000000)
    write(fixture / "old/data" / ASSET, b"ordinary attachment", 1700000000)
    first = run(False, "old", "01-old-initial", [{"op": "index", "memo": "old before appearance", "checkChunks": True}, {"op": send}])
    early_id = first["results"][0]["value"]["id"]
    run(True, "new-a", "02-new-bootstrap", [{"op": "index", "memo": "new bootstrap", "checkChunks": True}, {"op": receive}])
    theme = fixture / "new-a/data/themes/shared/theme.css"
    state = fixture / "new-a/data/storage/bazaar/themes/shared.json"
    write(theme, b"body { color: black; }", 1700000000)
    write(state, package_state(False), 1700000000)
    run(True, "new-a", "03-new-install", [{"op": "index", "memo": "new install", "checkChunks": True}, {"op": send}])
    run(False, "old", "04-old-receives-events", [{"op": receive}, {"op": "inspect"}])
    assert list((fixture / "old/data/storage/appearance-v1").rglob("*.sypkg"))
    write(fixture / "old/data" / NOTE, edited, 1700000002)
    run(False, "old", "05-old-edit-sync", [{"op": "index", "memo": "old note edit", "checkChunks": True}, {"op": send}])
    run(True, "new-a", "06-new-receives-old-note", [{"op": receive}])
    assert (fixture / "new-a/data" / NOTE).read_bytes() == edited
    theme.unlink()
    write(state, package_state(True), 1700000003)
    run(True, "new-a", "07-new-delete", [{"op": "index", "memo": "new uninstall", "checkChunks": True}, {"op": send}])
    run(False, "old", "08-old-receives-tombstone", [{"op": receive}, {"op": "inspect"}])
    assert len(list((fixture / "old/data/storage/appearance-v1").rglob("*.sypkg"))) == 2
    root_ref = fixture / "cloud/main/refs/tags/.siyuan-appearance-v1"
    root_before = root_ref.read_bytes()
    run(False, "old", "09-old-early-checkout", [{"op": "checkout", "id": early_id}])
    assert not list((fixture / "old/data/storage/appearance-v1").rglob("*.sypkg"))
    write(fixture / "old/data" / NOTE, after_checkout, 1700000004)
    latest = run(False, "old", "10-old-sync-without-events", [{"op": "index", "memo": "old edit after checkout", "checkChunks": True}, {"op": send}, {"op": "inspect"}])
    assert not any("/storage/appearance-v1/" in row["file"]["path"] for row in latest["results"][-1]["value"]["files"])
    run(False, "old", "11-old-local-purge", [{"op": "purge"}])
    if check_original_local:
        run(False, "old", "12-original-local-cloud-purge", [{"op": "purgeCloud"}], expect_failure=True)
    run(False, "old", "13-old-cloud-purge", [{"op": "purgeCloud"}], recursive_refs=True)
    assert root_ref.read_bytes() == root_before
    run(True, "new-c", "14-new-restores-root", [{"op": "index", "memo": "fresh new client", "checkChunks": True}, {"op": receive}])
    assert (fixture / "new-c/data" / NOTE).read_bytes() == after_checkout
    recovered = json.loads((fixture / "new-c/data/storage/bazaar/themes/shared.json").read_text(encoding="utf-8"))
    assert recovered["deleted"] is True
    assert not (fixture / "new-c/data/themes/shared/theme.css").exists()
    assert len(list((fixture / "new-c/data/storage/appearance-v1").rglob("*.sypkg"))) == 2
    assert (fixture / "new-c/data" / ASSET).exists() is not ondemand
    return {"case": case, "earlyIndex": early_id, "rootIndex": root_before.decode(),
            "noteSHA256": hashlib.sha256(after_checkout).hexdigest(), "fixture": str(fixture)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=CASES, action="append", help="run selected cases instead of the full matrix")
    args = parser.parse_args()
    root = Path(tempfile.mkdtemp(prefix="siyuan-appearance-compat-"))
    print("Artifacts: " + str(root), flush=True)
    module = json.loads(go(["list", "-m", "-json", "github.com/siyuan-note/dejavu"], OLD_DRIVER))
    if module.get("Version") != OLD_VERSION or module.get("Replace") or not module.get("Sum"):
        raise RuntimeError("legacy client did not resolve the exact unmodified published module: " + json.dumps(module))
    report = {"legacyModule": module, "goVersion": go(["version"], OLD_DRIVER).strip(),
              "legacyModuleVerification": go(["mod", "verify"], OLD_DRIVER, timeout=900).strip(),
              "cloudPurgeBoundary": "Original Local refs-directory failure is recorded; GC cases use only recursive-ref-listing provider adapter; legacy engine unchanged",
              "cases": []}
    try:
        new_driver = prepare_new_driver(root)
        for i, case in enumerate(args.case or CASES):
            report["cases"].append(run_case(root, new_driver, case, i == 0))
    except Exception as error:
        report["error"] = str(error)
        raise
    finally:
        (root / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"passed": [case["case"] for case in report["cases"]], "report": str(root / "report.json")}, indent=2), flush=True)


if __name__ == "__main__":
    main()
