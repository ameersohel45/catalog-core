package catalog

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/beckn/catalog-core/pkg/crawler"
)

func changeDoc(t *testing.T, upsertID string) []byte {
	t.Helper()
	item, err := json.Marshal(map[string]any{"id": upsertID, "name": upsertID})
	if err != nil {
		t.Fatal(err)
	}
	doc := ChangeFileDoc{Resources: DiffBlock{Upserts: []json.RawMessage{item}}}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestResolve_FoldsChangesInOrder(t *testing.T) {
	baseline := json.RawMessage(`{"id":"c1","resources":[]}`)
	changes := []FileEntry{{URL: "v2", FromVersion: 1, ToVersion: 2}, {URL: "v3", FromVersion: 2, ToVersion: 3}}
	docs := map[int64][]byte{2: changeDoc(t, "r1"), 3: changeDoc(t, "r2")}

	got, err := Resolve(baseline, 1, changes, func(ch FileEntry) ([]byte, error) {
		return docs[ch.ToVersion], nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Resources []struct {
			ID string `json:"id"`
		} `json:"resources"`
	}
	if err := json.Unmarshal(got, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Resources) != 2 || doc.Resources[0].ID != "r1" || doc.Resources[1].ID != "r2" {
		t.Fatalf("resources = %+v, want [r1 r2] applied in order", doc.Resources)
	}
}

// TestResolve_SortsOutOfOrderChanges proves the chain folds correctly even
// when changes isn't already sorted by version -- a caller must not assume
// index-declared order is contiguous.
func TestResolve_SortsOutOfOrderChanges(t *testing.T) {
	baseline := json.RawMessage(`{"id":"c1","resources":[]}`)
	changes := []FileEntry{{URL: "v3", FromVersion: 2, ToVersion: 3}, {URL: "v2", FromVersion: 1, ToVersion: 2}}
	docs := map[int64][]byte{2: changeDoc(t, "r1"), 3: changeDoc(t, "r2")}

	got, err := Resolve(baseline, 1, changes, func(ch FileEntry) ([]byte, error) {
		return docs[ch.ToVersion], nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Resources []struct {
			ID string `json:"id"`
		} `json:"resources"`
	}
	if err := json.Unmarshal(got, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Resources) != 2 || doc.Resources[0].ID != "r1" || doc.Resources[1].ID != "r2" {
		t.Fatalf("resources = %+v, want [r1 r2] applied in ascending version order despite input order", doc.Resources)
	}
}

func TestResolve_GapInChainIsPermanentFaultGap(t *testing.T) {
	baseline := json.RawMessage(`{"id":"c1","resources":[]}`)
	// Missing the 1->2 change: this jumps straight to 2->3.
	changes := []FileEntry{{URL: "v3", FromVersion: 2, ToVersion: 3}}
	_, err := Resolve(baseline, 1, changes, func(ch FileEntry) ([]byte, error) {
		return changeDoc(t, "r1"), nil
	}, nil)
	if err == nil || !crawler.IsPermanent(err) {
		t.Fatalf("err = %v, want a permanent gap error", err)
	}
	if crawler.PermanentClass(err) != FaultGap {
		t.Fatalf("class = %q, want %q", crawler.PermanentClass(err), FaultGap)
	}
}

func TestResolve_PlaceholderWithNoURLIsSkipped(t *testing.T) {
	baseline := json.RawMessage(`{"id":"c1","resources":[]}`)
	// A not-yet-published placeholder (no URL) must not break the chain or
	// get fetched.
	changes := []FileEntry{{FromVersion: 1, ToVersion: 2}} // no URL
	_, err := Resolve(baseline, 1, changes, func(FileEntry) ([]byte, error) {
		t.Fatal("fetch should not be called for a placeholder with no URL")
		return nil, nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestResolve_NoChangesReturnsBaselineUnchanged(t *testing.T) {
	baseline := json.RawMessage(`{"id":"c1","resources":[]}`)
	got, err := Resolve(baseline, 1, nil, func(FileEntry) ([]byte, error) {
		t.Fatal("fetch should not be called with no changes")
		return nil, nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(baseline) {
		t.Fatalf("got %s, want baseline unchanged", got)
	}
}

func TestResolve_FetchErrorStopsFolding(t *testing.T) {
	baseline := json.RawMessage(`{"id":"c1","resources":[]}`)
	changes := []FileEntry{{URL: "v2", FromVersion: 1, ToVersion: 2}}
	wantErr := errors.New("network blip")
	_, err := Resolve(baseline, 1, changes, func(FileEntry) ([]byte, error) {
		return nil, wantErr
	}, nil)
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want wrapping %v", err, wantErr)
	}
}

func TestResolve_OnAppliedCalledPerChangeWithoutRefetching(t *testing.T) {
	baseline := json.RawMessage(`{"id":"c1","resources":[]}`)
	changes := []FileEntry{{URL: "v2", FromVersion: 1, ToVersion: 2}, {URL: "v3", FromVersion: 2, ToVersion: 3}}
	fetchCount := 0
	docs := map[int64][]byte{2: changeDoc(t, "r1"), 3: changeDoc(t, "r2")}

	var seen []int64
	_, err := Resolve(baseline, 1, changes, func(ch FileEntry) ([]byte, error) {
		fetchCount++
		return docs[ch.ToVersion], nil
	}, func(ch FileEntry, raw []byte) {
		seen = append(seen, ch.ToVersion)
		if len(raw) == 0 {
			t.Fatal("onApplied should receive the already-fetched raw bytes")
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if fetchCount != 2 {
		t.Fatalf("fetchCount = %d, want 2 (no re-fetching for onApplied)", fetchCount)
	}
	if len(seen) != 2 || seen[0] != 2 || seen[1] != 3 {
		t.Fatalf("seen = %v, want [2 3]", seen)
	}
}

// TestResolve_SupersededChangeIsSkippedNotAGap covers exactly the scenario a
// publisher's compaction produces: the baseline has already advanced past a
// change file's own version (its content is already folded into the new
// baseline), but the publisher still lists it for a grace period. Resolve
// must skip it silently -- not treat its now-stale fromVersion as a broken
// chain -- and must not re-fetch or re-apply its (already-folded) content.
func TestResolve_SupersededChangeIsSkippedNotAGap(t *testing.T) {
	baseline := json.RawMessage(`{"id":"c1","resources":[]}`)
	changes := []FileEntry{
		{URL: "v1-to-v2-superseded", FromVersion: 1, ToVersion: 2}, // pre-compaction, already folded into baseline@5
		{URL: "v5-to-v6", FromVersion: 5, ToVersion: 6},
	}
	fetchCount := 0
	got, err := Resolve(baseline, 5, changes, func(ch FileEntry) ([]byte, error) {
		fetchCount++
		if ch.ToVersion == 2 {
			t.Fatal("must not fetch a superseded change")
		}
		return changeDoc(t, "r1"), nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if fetchCount != 1 {
		t.Fatalf("fetchCount = %d, want 1 (only the real change past the baseline)", fetchCount)
	}
	var doc struct {
		Resources []struct {
			ID string `json:"id"`
		} `json:"resources"`
	}
	if err := json.Unmarshal(got, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Resources) != 1 || doc.Resources[0].ID != "r1" {
		t.Fatalf("resources = %+v, want just [r1] from the real change", doc.Resources)
	}
}

// TestResolve_SupersededChangeStillReportedToOnApplied lets a caller (e.g.
// pkg/catalog/store's grace-period retention) still decide whether to keep
// listing a superseded change, without Resolve fetching it.
func TestResolve_SupersededChangeStillReportedToOnApplied(t *testing.T) {
	baseline := json.RawMessage(`{"id":"c1","resources":[]}`)
	changes := []FileEntry{{URL: "v1-to-v2-superseded", FromVersion: 1, ToVersion: 2}}
	var seen []int64
	var sawNilRaw bool
	_, err := Resolve(baseline, 5, changes, func(FileEntry) ([]byte, error) {
		t.Fatal("must not fetch a superseded change")
		return nil, nil
	}, func(ch FileEntry, raw []byte) {
		seen = append(seen, ch.ToVersion)
		sawNilRaw = raw == nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(seen) != 1 || seen[0] != 2 {
		t.Fatalf("seen = %v, want [2] (onApplied still called for the superseded change)", seen)
	}
	if !sawNilRaw {
		t.Fatal("expected nil raw for a superseded change (never fetched)")
	}
}
