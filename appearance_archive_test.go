package dejavu

import (
	"archive/zip"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

func appearanceArchiveTestState(t *testing.T, files map[string][]byte, deleted bool) []byte {
	t.Helper()
	digests := map[string]string{}
	for name, data := range files {
		digests[name] = appearanceArchiveDigest(data)
	}
	state, err := json.MarshalIndent(map[string]any{
		"version": 1, "deleted": deleted, "migration": true, "files": digests,
		"repoURL": "owner/example", "installTime": 123, "futurePackageInfo": map[string]any{"preserve": true},
	}, "", "\t")
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func appearanceArchiveTestPackage(t *testing.T) *appearanceArchive {
	t.Helper()
	files := map[string][]byte{
		"theme.css":          []byte("body { color: black; }"),
		"fonts/custom.woff2": {0, 1, 2, 3},
		"empty.txt":          {},
	}
	return &appearanceArchive{Key: "/themes/custom", Parents: []string{strings.Repeat("b", 64), strings.Repeat("a", 64)},
		State: appearanceArchiveTestState(t, files, false), Files: files}
}

func TestAppearanceArchiveDeterministicRoundTrip(t *testing.T) {
	archive := appearanceArchiveTestPackage(t)
	state := append([]byte{}, archive.State...)
	data, err := encodeAppearanceArchive(archive)
	if err != nil {
		t.Fatal(err)
	}
	if archive.Digest != appearanceArchiveDigest(data) || !sort.StringsAreSorted(archive.Parents) {
		t.Fatal("encoder did not publish canonical parents and digest")
	}
	again := appearanceArchiveTestPackage(t)
	again.Files = map[string][]byte{"empty.txt": {}, "fonts/custom.woff2": {0, 1, 2, 3}, "theme.css": []byte("body { color: black; }")}
	again.Parents = []string{strings.Repeat("a", 64), strings.Repeat("b", 64)}
	repeated, err := encodeAppearanceArchive(again)
	if err != nil || !bytes.Equal(data, repeated) {
		t.Fatalf("map or parent order changed immutable bytes: %v", err)
	}
	key, digest := appearanceArchiveKey("/storage/appearance-v1" + archive.Key + "/" + archive.Digest + ".sypkg")
	if key != archive.Key || digest != archive.Digest {
		t.Fatal("archive path did not preserve key and digest")
	}
	decoded, err := decodeAppearanceArchive(key, digest, data)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Key != archive.Key || decoded.Digest != archive.Digest || !reflect.DeepEqual(decoded.Files, archive.Files) ||
		!reflect.DeepEqual(decoded.Parents, archive.Parents) || !bytes.Equal(decoded.State, state) || !bytes.Equal(archive.State, state) {
		t.Fatal("round trip changed package state, payload, or lineage")
	}
	reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	previous := ""
	for _, file := range reader.File {
		if file.Name <= previous || file.Method != zip.Store || file.Mode() != 0644 || !file.Modified.Equal(time.UnixMilli(appearanceEventModified)) {
			t.Fatalf("archive header is not deterministic: %+v", file.FileHeader)
		}
		previous = file.Name
	}
}

func TestAppearanceArchiveDeletion(t *testing.T) {
	archive := &appearanceArchive{Key: "/icons/custom", State: appearanceArchiveTestState(t, nil, true)}
	data, err := encodeAppearanceArchive(archive)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeAppearanceArchive(archive.Key, archive.Digest, data)
	if err != nil || decoded.Parents == nil || len(decoded.Parents) != 0 || len(decoded.Files) != 0 {
		t.Fatalf("deleted event did not round trip: %v", err)
	}
	entries := appearanceArchiveTestEntries(t, data)
	if len(entries) != 2 {
		t.Fatal("deleted archive contains payload")
	}
	var manifest map[string]any
	if err = json.Unmarshal(entries[0].data, &manifest); err != nil || manifest["parents"] == nil {
		t.Fatal("initial event encoded null parents")
	}
}

func TestAppearanceArchiveUnicodeResourceNames(t *testing.T) {
	files := map[string][]byte{
		"images/👩‍👩‍👧‍👦.png": []byte("family"),
		"images/✈️.png":      []byte("variation selector"),
	}
	archive := &appearanceArchive{Key: "/themes/custom", Files: files, State: appearanceArchiveTestState(t, files, false)}
	data, err := encodeAppearanceArchive(archive)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeAppearanceArchive(archive.Key, archive.Digest, data)
	if err != nil || !reflect.DeepEqual(decoded.Files, files) {
		t.Fatalf("legal Unicode resource names did not round trip: %v", err)
	}
	files = map[string][]byte{"images/control\x1f.png": []byte("invalid")}
	archive = &appearanceArchive{Key: "/themes/custom", Files: files, State: appearanceArchiveTestState(t, files, false)}
	if _, err = encodeAppearanceArchive(archive); err == nil {
		t.Fatal("Windows control character was accepted")
	}
}

func TestAppearanceArchiveKeySafety(t *testing.T) {
	digest := strings.Repeat("a", 64)
	for _, name := range []string{"custom", "custom-theme", "主题", "theme.v1"} {
		key, got := appearanceArchiveKey("/storage/appearance-v1/themes/" + name + "/" + digest + ".sypkg")
		if key != "/themes/"+name || got != digest {
			t.Errorf("valid package name rejected: %s", name)
		}
	}
	for _, p := range []string{
		"storage/appearance-v1/themes/custom/" + digest + ".sypkg",
		"/storage/appearance-v2/themes/custom/" + digest + ".sypkg",
		"/storage/appearance-v1/themes/../" + digest + ".sypkg",
		"/storage/appearance-v1/themes/.hidden/" + digest + ".sypkg",
		"/storage/appearance-v1/themes/daylight/" + digest + ".sypkg",
		"/storage/appearance-v1/icons/LITHENESS/" + digest + ".sypkg",
		"/storage/appearance-v1/plugins/custom/" + digest + ".sypkg",
		"/storage/appearance-v1/themes/custom/" + strings.Repeat("A", 64) + ".sypkg",
		"/storage/appearance-v1/themes/custom/" + strings.Repeat("z", 64) + ".sypkg",
		"/storage/appearance-v1/themes/custom/short.sypkg",
		"/storage/appearance-v1/themes/custom/" + digest + ".sypkg/extra",
	} {
		if key, got := appearanceArchiveKey(p); key != "" || got != "" {
			t.Errorf("unsafe archive path accepted: %s", p)
		}
	}
	for _, name := range []string{"CON", "con.txt", "LPT1", "COM¹", "bad.", "bad ", " bad", "bad:name", "bad\\name", "bad\x00name", "bad..name"} {
		if key, _ := appearanceArchiveKey("/storage/appearance-v1/themes/" + name + "/" + digest + ".sypkg"); key != "" {
			t.Errorf("nonportable package name accepted: %q", name)
		}
	}
}

func TestAppearanceArchivePayloadSafety(t *testing.T) {
	for _, name := range []string{"../outside", "/absolute", "a/../../outside", "a\\b", "a:b", "a//b", "a/./b", "dir/", ".git/config", ".siyuan", "nested/.siyuan", ".siyuan/.draft", ".siyuan/private.tmp", "private.tmp", "CON.txt", "NUL", "COM².txt", "name.", "name ", "a/PRN/file", "a\x00b"} {
		t.Run(name, func(t *testing.T) {
			files := map[string][]byte{name: []byte("data")}
			archive := &appearanceArchive{Key: "/themes/custom", State: appearanceArchiveTestState(t, files, false), Files: files}
			if _, err := encodeAppearanceArchive(archive); err == nil {
				t.Fatalf("unsafe payload path accepted: %q", name)
			}
		})
	}
	for _, names := range [][]string{
		{"a.css", "A.css"}, {"fonts/a", "Fonts/b"}, {"a", "a/b"}, {"a/b", "A"},
		{"é.css", "e\u0301.css"}, {"Σ.css", "ς.css"},
	} {
		t.Run(strings.Join(names, "+"), func(t *testing.T) {
			files := map[string][]byte{}
			for _, name := range names {
				files[name] = []byte("data")
			}
			archive := &appearanceArchive{Key: "/themes/custom", State: appearanceArchiveTestState(t, files, false), Files: files}
			if _, err := encodeAppearanceArchive(archive); err == nil {
				t.Fatal("cross-platform file collision accepted")
			}
		})
	}
}

type appearanceArchiveTestEntry struct {
	name     string
	data     []byte
	method   uint16
	mode     os.FileMode
	modified time.Time
}

func appearanceArchiveTestEntries(t *testing.T, data []byte) []appearanceArchiveTestEntry {
	t.Helper()
	reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	var entries []appearanceArchiveTestEntry
	for _, file := range reader.File {
		entry, err := file.Open()
		if err != nil {
			t.Fatal(err)
		}
		content, err := io.ReadAll(entry)
		entry.Close()
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, appearanceArchiveTestEntry{file.Name, content, file.Method, file.Mode(), file.Modified})
	}
	return entries
}

func appearanceArchiveTestZIP(t *testing.T, entries []appearanceArchiveTestEntry) []byte {
	t.Helper()
	var data bytes.Buffer
	writer := zip.NewWriter(&data)
	for _, entry := range entries {
		header := &zip.FileHeader{Name: entry.name, Method: entry.method, Modified: entry.modified}
		header.SetMode(entry.mode)
		file, err := writer.CreateHeader(header)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = file.Write(entry.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return data.Bytes()
}

func TestAppearanceArchiveRejectsMalformedZIP(t *testing.T) {
	archive := appearanceArchiveTestPackage(t)
	data, err := encodeAppearanceArchive(archive)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func([]appearanceArchiveTestEntry) []appearanceArchiveTestEntry{
		"duplicate": func(entries []appearanceArchiveTestEntry) []appearanceArchiveTestEntry {
			return append(entries, entries[0])
		},
		"extra": func(entries []appearanceArchiveTestEntry) []appearanceArchiveTestEntry {
			return append(entries, appearanceArchiveTestEntry{"unknown.json", []byte("{}"), zip.Store, 0644, time.UnixMilli(appearanceEventModified)})
		},
		"missing-state": func(entries []appearanceArchiveTestEntry) []appearanceArchiveTestEntry {
			return entries[:len(entries)-1]
		},
		"unsafe-path": func(entries []appearanceArchiveTestEntry) []appearanceArchiveTestEntry {
			entries[1].name = "files/../escape"
			return entries
		},
		"symlink": func(entries []appearanceArchiveTestEntry) []appearanceArchiveTestEntry {
			entries[1].mode = os.ModeSymlink | 0644
			return entries
		},
		"directory": func(entries []appearanceArchiveTestEntry) []appearanceArchiveTestEntry {
			entries[1].mode = os.ModeDir | 0755
			entries[1].name = "files/directory/"
			return entries
		},
		"compressed": func(entries []appearanceArchiveTestEntry) []appearanceArchiveTestEntry {
			entries[1].method = zip.Deflate
			return entries
		},
		"mtime": func(entries []appearanceArchiveTestEntry) []appearanceArchiveTestEntry {
			entries[1].modified = time.Unix(1, 0)
			return entries
		},
		"mode": func(entries []appearanceArchiveTestEntry) []appearanceArchiveTestEntry {
			entries[1].mode = 0777
			return entries
		},
		"order": func(entries []appearanceArchiveTestEntry) []appearanceArchiveTestEntry {
			entries[0], entries[1] = entries[1], entries[0]
			return entries
		},
		"payload": func(entries []appearanceArchiveTestEntry) []appearanceArchiveTestEntry {
			entries[2].data = []byte("changed")
			return entries
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := appearanceArchiveTestZIP(t, mutate(appearanceArchiveTestEntries(t, data)))
			if _, err := decodeAppearanceArchive(archive.Key, appearanceArchiveDigest(changed), changed); err == nil {
				t.Fatal("malformed ZIP accepted")
			}
		})
	}
	corrupt := append([]byte{}, data...)
	corrupt[len(corrupt)/2] ^= 1
	if _, err = decodeAppearanceArchive(archive.Key, archive.Digest, corrupt); err == nil {
		t.Fatal("archive digest mismatch accepted")
	}
	if _, err = decodeAppearanceArchive("/themes/other", archive.Digest, data); err == nil {
		t.Fatal("manifest key substitution accepted")
	}
}

func TestAppearanceArchiveChecksCRCAtDeclaredEnd(t *testing.T) {
	for _, contents := range []string{"", "stored resource content"} {
		files := map[string][]byte{"resource.bin": []byte(contents)}
		archive := &appearanceArchive{Key: "/themes/crc", Files: files, State: appearanceArchiveTestState(t, files, false)}
		data, err := encodeAppearanceArchive(archive)
		if err != nil {
			t.Fatal(err)
		}
		corrupt := append([]byte(nil), data...)
		changed := false
		for offset := bytes.Index(corrupt, []byte{'P', 'K', 1, 2}); offset >= 0 && offset+46 <= len(corrupt); {
			nameSize := int(binary.LittleEndian.Uint16(corrupt[offset+28:]))
			extraSize := int(binary.LittleEndian.Uint16(corrupt[offset+30:]))
			commentSize := int(binary.LittleEndian.Uint16(corrupt[offset+32:]))
			if string(corrupt[offset+46:offset+46+nameSize]) == "files/resource.bin" {
				corrupt[offset+16] ^= 1
				changed = true
				break
			}
			offset += 46 + nameSize + extraSize + commentSize
		}
		if !changed {
			t.Fatal("fixture has no resource central directory")
		}
		if _, err = decodeAppearanceArchive(archive.Key, appearanceArchiveDigest(corrupt), corrupt); !errors.Is(err, zip.ErrChecksum) {
			t.Fatalf("entry checksum was not checked at its declared end: %v", err)
		}
	}
}

func BenchmarkDecodeAppearanceArchiveStored(b *testing.B) {
	resource := bytes.Repeat([]byte{0x5a}, 20<<20)
	files := map[string][]byte{"assets/resource.bin": resource}
	state, err := json.Marshal(appearanceRecord{Version: 1, Files: map[string]string{
		"assets/resource.bin": appearanceArchiveDigest(resource),
	}})
	if err != nil {
		b.Fatal(err)
	}
	archive := &appearanceArchive{Key: "/themes/benchmark", State: state, Files: files}
	data, err := encodeAppearanceArchive(archive)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err = decodeAppearanceArchive(archive.Key, archive.Digest, data); err != nil {
			b.Fatal(err)
		}
	}
}

func TestAppearanceArchiveRejectsManifestAndState(t *testing.T) {
	archive := appearanceArchiveTestPackage(t)
	data, err := encodeAppearanceArchive(archive)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{"appearance.json", "state.json"} {
		mutations := map[string]func(map[string]any){
			"version": func(value map[string]any) { value["version"] = 99 },
		}
		if target == "appearance.json" {
			mutations["null-parents"] = func(value map[string]any) { value["parents"] = nil }
			mutations["unsorted-parents"] = func(value map[string]any) {
				value["parents"] = []string{strings.Repeat("b", 64), strings.Repeat("a", 64)}
			}
			mutations["duplicate-parents"] = func(value map[string]any) {
				value["parents"] = []string{strings.Repeat("a", 64), strings.Repeat("a", 64)}
			}
			mutations["bad-parent"] = func(value map[string]any) { value["parents"] = []string{"invalid"} }
			mutations["unknown-field"] = func(value map[string]any) { value["unexpected"] = true }
			mutations["wrong-name"] = func(value map[string]any) { value["name"] = "other" }
		} else {
			mutations["null-files"] = func(value map[string]any) { value["files"] = nil }
			mutations["empty-files"] = func(value map[string]any) { value["files"] = map[string]string{} }
			mutations["deleted-payload"] = func(value map[string]any) { value["deleted"] = true }
			mutations["invalid-migration"] = func(value map[string]any) { value["migration"] = "true" }
			mutations["missing-file"] = func(value map[string]any) { value["files"].(map[string]any)["missing"] = strings.Repeat("a", 64) }
			mutations["wrong-digest"] = func(value map[string]any) { value["files"].(map[string]any)["theme.css"] = strings.Repeat("a", 64) }
		}
		for name, mutate := range mutations {
			t.Run(target+"/"+name, func(t *testing.T) {
				entries := appearanceArchiveTestEntries(t, data)
				for i := range entries {
					if entries[i].name == target {
						var value map[string]any
						if err := json.Unmarshal(entries[i].data, &value); err != nil {
							t.Fatal(err)
						}
						mutate(value)
						entries[i].data, err = json.Marshal(value)
						if err != nil {
							t.Fatal(err)
						}
					}
				}
				changed := appearanceArchiveTestZIP(t, entries)
				if _, err := decodeAppearanceArchive(archive.Key, appearanceArchiveDigest(changed), changed); err == nil {
					t.Fatal("invalid archive JSON accepted")
				}
			})
		}
	}
	archive.State = []byte(`{"version":99,"version":1,"deleted":true,"migration":false,"files":{}}`)
	archive.Files = nil
	if _, err = encodeAppearanceArchive(archive); err == nil {
		t.Fatal("duplicate state fields accepted")
	}
	archive = appearanceArchiveTestPackage(t)
	archive.Parents = []string{strings.Repeat("a", 64), strings.Repeat("a", 64)}
	if _, err = encodeAppearanceArchive(archive); err != nil || !reflect.DeepEqual(archive.Parents, []string{strings.Repeat("a", 64)}) {
		t.Fatal("encoder did not deduplicate parents")
	}
}
