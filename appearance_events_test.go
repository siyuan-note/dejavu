package dejavu

import "testing"

func TestAppearanceEventUnionRejectsPackageAliases(t *testing.T) {
	for _, keys := range [][2]string{{"/themes/Foo", "/themes/foo"}, {"/themes/caf\u00e9", "/themes/cafe\u0301"}} {
		sets := []appearanceEventSet{}
		for _, key := range keys {
			sets = append(sets, appearanceEventSet{key: {"a": {appearanceArchive: &appearanceArchive{Key: key, Digest: "a"}}}})
		}
		if _, err := unionAppearanceEvents(sets...); err == nil {
			t.Fatalf("cross-device aliases accepted: %v", keys)
		}
	}
}

func TestAppearancePrepareDoesNotPromoteMigrationOverTombstone(t *testing.T) {
	repo := newAppearanceSyncTestRepo(t, t.TempDir(), t.TempDir(), "late", false)
	key := "/themes/example"
	writeAppearanceTestPackage(t, repo, key, map[string]string{"theme.css": "old local migration"}, 10, true)
	if err := repo.prepareAppearanceEvents(); err != nil {
		t.Fatal(err)
	}
	events, err := repo.readAppearanceDiskEvents()
	if err != nil {
		t.Fatal(err)
	}
	migration := selectAppearanceEvent(events[key], nil)
	deleted, err := appearanceDeletedArchive(key, migration.appearanceArchive)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = repo.writeAppearanceArchive(deleted); err != nil {
		t.Fatal(err)
	}
	if err = repo.prepareAppearanceEvents(); err != nil {
		t.Fatal(err)
	}
	after, err := repo.readAppearanceDiskEvents()
	if err != nil {
		t.Fatal(err)
	}
	if len(after[key]) != 2 || !appearanceEventRecord(selectAppearanceEvent(after[key], nil)).Deleted {
		t.Fatal("unchanged migration head was promoted into an explicit reinstall")
	}
}
