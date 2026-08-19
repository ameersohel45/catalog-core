package catalog

// change.go — change detection: compares a catalog's index entry against a
// caller's stored cursors and yields a typed Decision (sync / skip_unchanged
// / retire / rollback). Pure, no I/O. Ported from the catalog-crawler
// prototype's own change.go.

// Action is what a caller should do with a catalog on one poll.
type Action string

const (
	ActionSync          Action = "sync"           // entryVersion (or content) advanced; enqueue a sync
	ActionSkipUnchanged Action = "skip_unchanged" // entryVersion cursor already at latest
	ActionRetire        Action = "retire"         // catalog carries retiredAt
	ActionRollback      Action = "rollback"       // entryVersion or content version went backwards; flag, don't apply
)

// Decision is the outcome of change detection for one catalog.
type Decision struct {
	Action       Action
	ToVersion    int64 // content-lineage target (CatalogEntry.LatestVersion())
	EntryVersion int64 // the entry-level cursor to persist once this decision is acted on
}

// DetectChange compares entry against a caller's stored cursors and decides
// what to do. seen=false means the caller has never synced this catalog.
// entryCursor/contentCursor are the caller's last-persisted entry-level and
// content-lineage cursors.
//
// The two cursors are checked independently (CON-TBD-11 covers a regression
// in EITHER one): entryVersion (bumps on ANY change to the entry, content or
// metadata) is the cheap first check; the content-lineage version is what
// decides how much content actually needs resolving. A metadata-only change
// (e.g. networkIds edited, no new file) bumps entryVersion with the content
// version unchanged -- still ActionSync, so the entry is re-evaluated, but a
// caller's own fold produces an empty delta range for it.
func DetectChange(entry CatalogEntry, entryCursor, contentCursor int64, seen bool) Decision {
	if seen && entry.EntryVersion < entryCursor {
		return Decision{Action: ActionRollback, ToVersion: entry.LatestVersion(), EntryVersion: entry.EntryVersion}
	}
	if entry.IsRetired() {
		return Decision{Action: ActionRetire, EntryVersion: entry.EntryVersion}
	}
	latest := entry.LatestVersion()
	switch {
	case !seen:
		return Decision{Action: ActionSync, ToVersion: latest, EntryVersion: entry.EntryVersion}
	case latest < contentCursor:
		return Decision{Action: ActionRollback, ToVersion: latest, EntryVersion: entry.EntryVersion}
	case entry.EntryVersion == entryCursor:
		return Decision{Action: ActionSkipUnchanged, ToVersion: contentCursor, EntryVersion: entryCursor}
	default:
		return Decision{Action: ActionSync, ToVersion: latest, EntryVersion: entry.EntryVersion}
	}
}
