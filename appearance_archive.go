package dejavu

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

type appearanceArchive struct {
	Key     string
	Parents []string
	State   []byte
	Files   map[string][]byte
	Digest  string
}

type appearanceArchiveManifest struct {
	Version int      `json:"version"`
	Kind    string   `json:"kind"`
	Name    string   `json:"name"`
	Parents []string `json:"parents"`
}

// appearanceArchiveKey 仅识别具有规范目录名和完整内容摘要的归档路径。
func appearanceArchiveKey(p string) (key, digest string) {
	const prefix = "/storage/appearance-v1/"
	if !strings.HasPrefix(p, prefix) || path.Clean(p) != p {
		return "", ""
	}
	parts := strings.Split(strings.TrimPrefix(p, prefix), "/")
	if len(parts) != 3 || !strings.HasSuffix(parts[2], ".sypkg") {
		return "", ""
	}
	key = "/" + parts[0] + "/" + parts[1]
	digest = strings.TrimSuffix(parts[2], ".sypkg")
	if !validAppearanceArchiveKey(key) || !validAppearanceArchiveDigest(digest) {
		return "", ""
	}
	return key, digest
}

// encodeAppearanceArchive 固定归档顺序、时间和权限，并以最终字节摘要标识不可变事件。
func encodeAppearanceArchive(archive *appearanceArchive) ([]byte, error) {
	if archive == nil || !validAppearanceArchiveKey(archive.Key) {
		return nil, errors.New("invalid appearance archive key")
	}
	if err := validateAppearanceArchiveFiles(archive.State, archive.Files); err != nil {
		return nil, err
	}
	parents := append([]string{}, archive.Parents...)
	sort.Strings(parents)
	canonicalParents := make([]string, 0, len(parents))
	for _, parent := range parents {
		if !validAppearanceArchiveDigest(parent) {
			return nil, errors.New("invalid appearance archive parent")
		}
		if len(canonicalParents) == 0 || canonicalParents[len(canonicalParents)-1] != parent {
			canonicalParents = append(canonicalParents, parent)
		}
	}
	parents = canonicalParents
	parts := strings.Split(strings.TrimPrefix(archive.Key, "/"), "/")
	manifest := appearanceArchiveManifest{Version: 1, Kind: parts[0], Name: parts[1], Parents: parents}
	metadata, err := json.Marshal(&manifest)
	if err != nil {
		return nil, err
	}
	entries := map[string][]byte{"appearance.json": metadata, "state.json": archive.State}
	for name, data := range archive.Files {
		entries["files/"+name] = data
	}
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	var data bytes.Buffer
	writer := zip.NewWriter(&data)
	for _, name := range names {
		header := &zip.FileHeader{Name: name, Method: zip.Store, Modified: time.UnixMilli(appearanceEventModified).UTC()}
		header.SetMode(0644)
		entry, createErr := writer.CreateHeader(header)
		if createErr != nil {
			return nil, createErr
		}
		if _, err = entry.Write(entries[name]); err != nil {
			return nil, err
		}
	}
	if err = writer.Close(); err != nil {
		return nil, err
	}
	archive.Parents = parents
	archive.Digest = appearanceArchiveDigest(data.Bytes())
	return data.Bytes(), nil
}

// decodeAppearanceArchive 校验外层摘要、归档结构和状态声明的完整文件集，不访问文件系统。
func decodeAppearanceArchive(key, digest string, data []byte) (*appearanceArchive, error) {
	if !validAppearanceArchiveKey(key) || !validAppearanceArchiveDigest(digest) || appearanceArchiveDigest(data) != digest {
		return nil, errors.New("appearance archive digest or key mismatch")
	}
	reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}
	if len(reader.File) < 2 || reader.Comment != "" {
		return nil, errors.New("invalid appearance archive structure")
	}
	entries := map[string][]byte{}
	var total uint64
	previousName := ""
	for _, file := range reader.File {
		if file.Name <= previousName || entries[file.Name] != nil || file.Method != zip.Store ||
			!file.Mode().IsRegular() || file.Mode().Perm() != 0644 || file.Comment != "" ||
			!file.Modified.Equal(time.UnixMilli(appearanceEventModified).UTC()) ||
			file.Flags&1 != 0 || file.CompressedSize64 != file.UncompressedSize64 ||
			file.UncompressedSize64 > uint64(len(data))-total {
			return nil, fmt.Errorf("invalid appearance archive entry: %s", file.Name)
		}
		if file.Name != "appearance.json" && file.Name != "state.json" {
			if !strings.HasPrefix(file.Name, "files/") || !validAppearanceArchiveRelative(strings.TrimPrefix(file.Name, "files/")) {
				return nil, fmt.Errorf("unexpected appearance archive entry: %s", file.Name)
			}
		}
		total += file.UncompressedSize64
		entry, openErr := file.Open()
		if openErr != nil {
			return nil, openErr
		}
		content := make([]byte, int(file.UncompressedSize64))
		_, readErr := io.ReadFull(entry, content)
		if readErr == nil {
			// 读取至末尾以触发 ZIP 校验和检查，空文件也必须经过此步骤。
			var extra [1]byte
			n, tailErr := io.ReadFull(entry, extra[:])
			if tailErr != io.EOF || n != 0 {
				readErr = tailErr
				if readErr == nil {
					readErr = errors.New("appearance archive entry exceeds declared size")
				}
			}
		}
		closeErr := entry.Close()
		if readErr != nil {
			return nil, readErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		entries[file.Name] = content
		previousName = file.Name
	}
	if entries["appearance.json"] == nil || entries["state.json"] == nil {
		return nil, errors.New("appearance archive metadata is missing")
	}
	if err = validateAppearanceArchiveJSON(entries["appearance.json"]); err != nil {
		return nil, err
	}
	var manifestFields map[string]json.RawMessage
	if err = json.Unmarshal(entries["appearance.json"], &manifestFields); err != nil {
		return nil, err
	}
	if len(manifestFields) != 4 || manifestFields["version"] == nil || manifestFields["kind"] == nil ||
		manifestFields["name"] == nil || manifestFields["parents"] == nil {
		return nil, errors.New("invalid appearance archive manifest fields")
	}
	var manifest appearanceArchiveManifest
	decoder := json.NewDecoder(bytes.NewReader(entries["appearance.json"]))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&manifest); err != nil {
		return nil, err
	}
	if manifest.Version != 1 || "/"+manifest.Kind+"/"+manifest.Name != key || manifest.Parents == nil {
		return nil, errors.New("unsupported or invalid appearance archive manifest")
	}
	for i, parent := range manifest.Parents {
		if !validAppearanceArchiveDigest(parent) || parent == digest || (i > 0 && manifest.Parents[i-1] >= parent) {
			return nil, errors.New("invalid appearance archive parent order")
		}
	}
	archive := &appearanceArchive{Key: key, Parents: manifest.Parents, State: entries["state.json"], Files: map[string][]byte{}, Digest: digest}
	for name, content := range entries {
		if strings.HasPrefix(name, "files/") {
			archive.Files[strings.TrimPrefix(name, "files/")] = content
		}
	}
	if err = validateAppearanceArchiveFiles(archive.State, archive.Files); err != nil {
		return nil, err
	}
	return archive, nil
}

func validAppearanceArchiveKey(key string) bool {
	parts := strings.Split(key, "/")
	if len(parts) != 3 || parts[0] != "" || (parts[1] != "themes" && parts[1] != "icons") ||
		!validAppearanceName(parts[2]) || !validAppearanceArchiveComponent(parts[2]) ||
		strings.HasPrefix(parts[2], " ") || strings.Contains(parts[2], "..") || strings.ContainsAny(parts[2], "&'") {
		return false
	}
	return !(parts[1] == "themes" && (strings.EqualFold(parts[2], "daylight") || strings.EqualFold(parts[2], "midnight"))) &&
		!(parts[1] == "icons" && strings.EqualFold(parts[2], "litheness"))
}

func validAppearanceArchiveRelative(relative string) bool {
	if relative == "" || strings.HasPrefix(relative, "/") || path.Clean(relative) != relative || ignoredAppearanceRelative(relative, false) {
		return false
	}
	for _, component := range strings.Split(relative, "/") {
		if !validAppearanceArchiveComponent(component) {
			return false
		}
	}
	return true
}

func validAppearanceArchiveComponent(component string) bool {
	if !utf8.ValidString(component) || component == "" || component == "." || component == ".." ||
		len(component) > 255 || strings.HasSuffix(component, ".") || strings.HasSuffix(component, " ") ||
		strings.ContainsAny(component, "<>:\"/\\|?*\x00") {
		return false
	}
	for _, char := range component {
		if char < 32 {
			return false
		}
	}
	stem := strings.ToUpper(strings.Split(component, ".")[0])
	switch stem {
	case "CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$":
		return false
	}
	if strings.HasPrefix(stem, "COM") || strings.HasPrefix(stem, "LPT") {
		suffix := strings.TrimPrefix(strings.TrimPrefix(stem, "COM"), "LPT")
		if strings.Contains("123456789¹²³", suffix) && utf8.RuneCountInString(suffix) == 1 {
			return false
		}
	}
	return true
}

func validAppearanceArchiveDigest(digest string) bool {
	if len(digest) != 64 || strings.ToLower(digest) != digest {
		return false
	}
	_, err := hex.DecodeString(digest)
	return err == nil
}

func appearanceArchiveDigest(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func validateAppearanceArchiveFiles(state []byte, files map[string][]byte) error {
	if err := validateAppearanceArchiveJSON(state); err != nil {
		return err
	}
	var record appearanceRecord
	if err := json.Unmarshal(state, &record); err != nil {
		return err
	}
	if record.Version != 1 || record.Files == nil || (record.Deleted && len(record.Files) != 0) ||
		(!record.Deleted && len(record.Files) == 0) || len(record.Files) != len(files) {
		return errors.New("unsupported or incomplete appearance archive state")
	}
	if err := validateAppearanceArchivePaths(record.Files); err != nil {
		return err
	}
	for relative, expected := range record.Files {
		content, found := files[relative]
		if !found || appearanceArchiveDigest(content) != expected {
			return fmt.Errorf("invalid appearance archive file: %s", relative)
		}
	}
	return nil
}

func validateAppearanceArchivePaths(files map[string]string) error {
	type node struct {
		name string
		file bool
	}
	paths := map[string]node{}
	folder := cases.Fold()
	for relative, expected := range files {
		if !validAppearanceArchiveRelative(relative) || !validAppearanceArchiveDigest(expected) {
			return fmt.Errorf("invalid appearance archive file: %s", relative)
		}
		parts := strings.Split(relative, "/")
		for i := range parts {
			prefix := strings.Join(parts[:i+1], "/")
			folded := folder.String(norm.NFC.String(prefix))
			isFile := i == len(parts)-1
			if previous, exists := paths[folded]; exists {
				if previous.name != prefix || previous.file || isFile {
					return fmt.Errorf("appearance archive path collision: %s", relative)
				}
			} else {
				paths[folded] = node{name: prefix, file: isFile}
			}
		}
	}
	return nil
}

// validateAppearanceArchiveJSON 拒绝重复字段，避免同一状态在不同读取器中产生不同解释。
func validateAppearanceArchiveJSON(data []byte) error {
	if !utf8.Valid(data) {
		return errors.New("invalid UTF-8 appearance archive JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var readValue func(int) error
	readValue = func(depth int) error {
		if depth > 64 {
			return errors.New("appearance archive JSON nesting is too deep")
		}
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		switch token {
		case json.Delim('{'):
			keys := map[string]bool{}
			for decoder.More() {
				keyToken, keyErr := decoder.Token()
				if keyErr != nil {
					return keyErr
				}
				key, ok := keyToken.(string)
				if !ok || keys[key] {
					return errors.New("duplicate appearance archive JSON field")
				}
				keys[key] = true
				if err = readValue(depth + 1); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		case json.Delim('['):
			for decoder.More() {
				if err = readValue(depth + 1); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		}
		return nil
	}
	if err := readValue(0); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errors.New("unexpected trailing appearance archive JSON")
	}
	return nil
}
