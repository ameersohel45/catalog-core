package catalog

import "testing"

func TestDetectChange_NeverSeenIsSync(t *testing.T) {
	entry := CatalogEntry{CatalogID: "p/c", EntryVersion: 1, Baseline: FileEntry{Version: 1}}
	d := DetectChange(entry, 0, 0, false)
	if d.Action != ActionSync || d.ToVersion != 1 || d.EntryVersion != 1 {
		t.Fatalf("d = %+v, want sync to v1", d)
	}
}

func TestDetectChange_UnchangedEntryVersionIsSkip(t *testing.T) {
	entry := CatalogEntry{CatalogID: "p/c", EntryVersion: 1, Baseline: FileEntry{Version: 1}}
	d := DetectChange(entry, 1, 1, true)
	if d.Action != ActionSkipUnchanged {
		t.Fatalf("d = %+v, want skip_unchanged", d)
	}
}

func TestDetectChange_AdvancedEntryVersionIsSync(t *testing.T) {
	entry := CatalogEntry{CatalogID: "p/c", EntryVersion: 2, Baseline: FileEntry{Version: 1}}
	d := DetectChange(entry, 1, 1, true)
	if d.Action != ActionSync || d.ToVersion != 1 {
		t.Fatalf("d = %+v, want sync (metadata-only change, content unchanged)", d)
	}
}

func TestDetectChange_RetiredEntryIsRetire(t *testing.T) {
	entry := CatalogEntry{CatalogID: "p/c", EntryVersion: 2, RetiredAt: "2026-01-01T00:00:00Z"}
	d := DetectChange(entry, 1, 1, true)
	if d.Action != ActionRetire || d.EntryVersion != 2 {
		t.Fatalf("d = %+v, want retire", d)
	}
}

func TestDetectChange_EntryVersionRegressionIsRollback(t *testing.T) {
	entry := CatalogEntry{CatalogID: "p/c", EntryVersion: 1, Baseline: FileEntry{Version: 1}}
	d := DetectChange(entry, 3, 1, true) // stored cursor is ahead of what the index now declares
	if d.Action != ActionRollback {
		t.Fatalf("d = %+v, want rollback (entryVersion regressed)", d)
	}
}

func TestDetectChange_ContentVersionRegressionIsRollback(t *testing.T) {
	entry := CatalogEntry{CatalogID: "p/c", EntryVersion: 5, Baseline: FileEntry{Version: 1}} // latest=1
	d := DetectChange(entry, 4, 3, true)                                                      // stored content cursor (3) is ahead of latest (1)
	if d.Action != ActionRollback {
		t.Fatalf("d = %+v, want rollback (content version regressed)", d)
	}
}
