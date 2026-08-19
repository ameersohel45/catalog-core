package catalog

// resolve.go — Resolve is the shared "fold a baseline with its change files,
// in order" walk: exactly what a caller reconstructing a catalog's current
// content from an index entry needs, whether it is reading its own
// previously-written, already-trusted output (a storage backend replaying
// prior state) or reading untrusted publisher content that must be verified
// file by file as it's fetched (a crawler). Both walk the same baseline+
// changes shape and fold with the same Apply -- only how one change file's
// raw bytes are obtained, and what a caller does once it's applied, differ.
// Those differences are exactly what fetch and onApplied parameterize.

import (
	"fmt"
	"sort"

	"github.com/beckn/catalog-core/pkg/crawler"
)

// FaultGap classifies a continuity break in a catalog's change-file chain
// (see Resolve) -- a publisher-side data problem (a missing or
// out-of-sequence change file) that will not fix itself on retry. Defined
// here rather than in pkg/crawler because it is specific to this document
// shape's own chaining rule, not a generic fetch outcome; pkg/crawler's
// FaultClass is a plain string type precisely so a consumer can add classes
// like this one (see crawler.FaultClass's doc).
const FaultGap crawler.FaultClass = "gap"

// Resolve folds baseline's content -- itself at baselineVersion -- with each
// of changes' content, in order, via Apply. fetch obtains one change's raw
// (already decoded, not yet applied) bytes; how it does so -- a local blob
// read, a verified remote fetch -- is entirely the caller's concern and
// Resolve neither knows nor cares.
//
// changes is sorted by EffectiveVersion before folding (a caller must not
// assume index-declared order is already contiguous), and each change's own
// declared FromVersion is checked against where the chain has reached so
// far -- starting at baselineVersion -- before it is fetched: a gap (a
// missing change file, or one out of sequence) is reported as a permanent
// FaultGap rather than silently folding an incomplete or out-of-order
// chain. This check is cheap and authenticated before a single byte of the
// change itself is fetched, because FileEntry.FromVersion/ToVersion come
// from the enclosing index entry's own self-signed declaration.
//
// A change with no URL (a not-yet-published placeholder) is skipped as a
// no-op -- it carries no content and does not advance the chain.
//
// A change already superseded by the baseline (EffectiveVersion <=
// baselineVersion) is also skipped without being fetched or continuity-
// checked: right after a forced re-baseline (compaction), every prior
// change's content is already folded into the new baseline, so re-applying
// it would be a no-op -- but the publisher may still legitimately list it
// for a grace period after the compaction, and its own fromVersion predates
// the compaction, so checking it against the current chain position would
// misreport a real, intentional retention as a broken chain.
//
// onApplied, if non-nil, is called for every change that isn't a bare
// placeholder, superseded or not -- with the raw bytes fetch returned for a
// folded one, or nil for a superseded one (never fetched) -- so a caller
// needing its own per-change bookkeeping (e.g. deciding whether a
// superseded pre-compaction change file is still worth listing) sees every
// change, not just the ones actually folded, without fetching a superseded
// one just to learn about it.
func Resolve(baseline []byte, baselineVersion int64, changes []FileEntry, fetch func(FileEntry) ([]byte, error), onApplied func(FileEntry, []byte)) ([]byte, error) {
	sorted := append([]FileEntry(nil), changes...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].EffectiveVersion() < sorted[j].EffectiveVersion() })

	content := baseline
	running := baselineVersion
	for _, ch := range sorted {
		if ch.URL == "" {
			continue // not-yet-published placeholder entry: no content to fold
		}
		if ch.EffectiveVersion() <= baselineVersion {
			if onApplied != nil {
				onApplied(ch, nil)
			}
			continue
		}
		if ch.FromVersion != running {
			return nil, crawler.PermanentFaultf(FaultGap,
				"catalog: change v%d declares fromVersion=%d, expected %d (gap in change files)", ch.ToVersion, ch.FromVersion, running)
		}
		raw, err := fetch(ch)
		if err != nil {
			return nil, fmt.Errorf("catalog: fetching change %s: %w", ch.URL, err)
		}
		content, err = Apply(content, raw)
		if err != nil {
			return nil, fmt.Errorf("catalog: applying change %s: %w", ch.URL, err)
		}
		running = ch.EffectiveVersion()
		if onApplied != nil {
			onApplied(ch, raw)
		}
	}
	return content, nil
}
