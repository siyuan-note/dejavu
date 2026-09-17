package dejavu

import (
	"errors"
	"os"
	"testing"

	"github.com/siyuan-note/dejavu/entity"
)

func TestAppearanceDisabledKeepsOrdinaryFileBehavior(t *testing.T) {
	for _, onDemand := range []bool{false, true} {
		t.Run(map[bool]string{false: "full", true: "on-demand"}[onDemand], func(t *testing.T) {
			base, remote := t.TempDir(), t.TempDir()
			a := newAssetTestRepo(t, base, remote, "a", false)
			b := newAssetTestRepo(t, base, remote, "b", onDemand)
			writeAssetTestFile(t, a, "/themes/example/theme.css", "ordinary css", 10)
			writeAssetTestFile(t, a, "/themes/example/assets/font.woff", "ordinary asset", 10)
			writeAssetTestFile(t, a, "/storage/bazaar/themes/example.json", "arbitrary metadata", 10)
			syncAppearanceTestRepo(t, a)
			syncAppearanceTestRepo(t, b)
			if a.appearanceJournal || b.appearanceJournal {
				t.Fatal("disabled feature created appearance journal")
			}
			if got := readAppearanceTestFile(t, b, "/storage/bazaar/themes/example.json"); got != "arbitrary metadata" {
				t.Fatalf("ordinary metadata changed: %s", got)
			}
			if onDemand {
				if _, err := os.Stat(b.absPath("/themes/example/assets/font.woff")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("ordinary nested asset bypassed on-demand policy: %v", err)
				}
				if _, err := b.EnsureAsset("/themes/example/assets/font.woff", nil); err != nil {
					t.Fatal(err)
				}
			}
			old, err := b.Latest()
			if err != nil {
				t.Fatal(err)
			}
			files, err := b.GetFiles(old)
			if err != nil {
				t.Fatal(err)
			}
			for _, file := range files {
				if file.Path == appearanceFormatPath || file.ID != entity.NewFile(file.Path, file.Size, file.Updated).ID {
					t.Fatalf("disabled feature wrote appearance protocol: %+v", file)
				}
			}
			writeAssetTestFile(t, b, "/themes/example/theme.css", "changed css", 20)
			if _, err = b.Index("ordinary edit", true, nil); err != nil {
				t.Fatal(err)
			}
			if _, _, err = b.Checkout(old.ID, nil); err != nil {
				t.Fatal(err)
			}
			if got := readAppearanceTestFile(t, b, "/themes/example/theme.css"); got != "ordinary css" {
				t.Fatalf("ordinary checkout did not restore contents: %s", got)
			}
		})
	}
}
