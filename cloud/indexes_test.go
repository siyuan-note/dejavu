package cloud

import (
	"math"
	"slices"
	"testing"
)

func TestIndexesPage(t *testing.T) {
	entries := make([]*Index, pageSize+1)
	for i := range entries {
		entries[i] = &Index{}
	}
	indexes := Indexes{Indexes: entries}
	for _, tc := range []struct {
		name string
		page int
		want []*Index
	}{
		{"first", 1, entries[:pageSize]},
		{"last", 2, entries[pageSize:]},
		{"past last", 3, nil},
		{"zero", 0, nil},
		{"negative", -1, nil},
		{"largest", math.MaxInt, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, pages := indexes.page(tc.page)
			if pages != 2 || !slices.Equal(got, tc.want) {
				t.Fatalf("page %d: got %d entries across %d pages, want %d entries across 2 pages", tc.page, len(got), pages, len(tc.want))
			}
		})
	}
	if got, pages := (&Indexes{}).page(1); len(got) != 0 || pages != 0 {
		t.Fatalf("empty index: got %d entries across %d pages", len(got), pages)
	}
}
