// Package crawlmanager is the crawl-side orchestration layer: the two
// entry points (PollIndexes, SyncNext) that discover indexes, detect which
// catalogs changed, and resolve+hand off each changed catalog's current
// content -- built on pkg/catalog's Fetcher and Resolve, which stay
// backend- and destination-agnostic. Source (discovery) and Sink (handoff)
// are injected because multiple implementations of each are equally valid
// (a DeDi registry lookup vs. a static list; an HTTP push to Discovery vs.
// a local queue) -- unlike Fetcher, where exactly one implementation is
// correct. Store is a plain injected dependency too, but concrete for now:
// it is not designed as a swappable plugin surface yet, just enough of a
// seam to fake in tests.
package crawlmanager

import (
	"context"
	"encoding/json"
	"time"

	"github.com/beckn/catalog-core/pkg/catalog"
)

// Source discovers the index URLs currently in scope for a crawl. Injected
// so CrawlManager never hardcodes a discovery mechanism.
type Source interface {
	Discover(ctx context.Context) ([]IndexRef, error)
}

// IndexRef is one index Source wants crawled: its URL and the publishing
// participant's identity, as Source itself knows it (before the index is
// ever fetched -- the index's own NodeID, once fetched, is what Fetcher
// actually resolves signing keys against).
type IndexRef struct {
	ParticipantID string
	IndexURL      string
}

// Sink is where CrawlManager hands off one resolved catalog's current
// content -- an HTTP push to Discovery, a local queue, a file, or anything
// else are all equally valid destinations. Injected and transport-agnostic
// by name as well as by design.
//
// entry is the index entry SyncNext just resolved content from -- passed
// through (rather than just entry.CatalogID) because a real destination like
// Discovery needs entry's own catalogType/networkIds/schemaTypes to build a
// correct push, not just the bytes.
type Sink interface {
	Send(ctx context.Context, entry catalog.CatalogEntry, content []byte) (SinkOutcome, error)
}

// SinkOutcome reports what the destination did with one handoff, so
// CrawlManager can log/classify the result without needing to know the
// sink's own error shape.
type SinkOutcome struct {
	Accepted bool
	Reason   string // set when Accepted is false
}

// CatalogCursor is one catalog's crawl-tracking cursor: the content-lineage
// version and entry-level version CrawlManager has already synced up to.
// Distinct from pkg/catalog/store's CatalogState, which reconstructs
// content for the publish side -- this is bookkeeping only, no content.
//
// Descriptor/Provider/CatalogType are captured from a successful sync's own
// resolved content (via catalog.ExtractEnvelope) and persisted alongside the
// cursor, not re-fetched later -- a retired index entry drops every file
// reference (baseline/changes/latest) once retiredAt is set, so by the time
// a retirement is processed there is nothing left to fetch an envelope from.
// Retired marks a catalog Complete has settled as retired (after its wipe
// push was acked, or after confirming there was nothing to wipe).
type CatalogCursor struct {
	CatalogID     string
	IndexURL      string
	ParticipantID string
	Version       int64
	EntryVersion  int64
	Descriptor    json.RawMessage
	Provider      json.RawMessage
	CatalogType   string
	Retired       bool
	SyncedAt      time.Time
}

// IndexCursor is one index's crawl-tracking cursor: the conditional-GET
// validators from its last fetch, for change detection on the next poll.
type IndexCursor struct {
	IndexURL      string
	ParticipantID string
	ETag          string
	LastModified  string
}

// PassReport records one failed sync attempt against a catalog, for
// diagnostics (CountParked/CountTracked-style visibility is Store's own
// concern; this is the per-attempt detail).
type PassReport struct {
	CatalogID     string
	IndexURL      string
	ParticipantID string
	Error         string
	At            time.Time
}

// Op names what SyncNext should do with a queued item.
type Op string

const (
	OpSync   Op = "sync"   // resolve current content and hand it to Sink
	OpRetire Op = "retire" // wipe the catalog from Sink using its captured envelope
)

// QueueItem is one catalog queued after PollIndexes' change detection
// (catalog.DetectChange) decided it needs work. SyncNext re-fetches the
// index at claim time for a sync (rather than this package storing a
// snapshot of the entry) so a sync always acts on the catalog's current
// declared content, never a possibly-stale copy -- a retire instead reads
// its envelope from Store, since a retired entry's index no longer carries
// any content to re-fetch.
type QueueItem struct {
	CatalogID     string
	IndexURL      string
	ParticipantID string
	Op            Op
}

// ClaimedItem is one QueueItem claimed for processing: id/ClaimID are
// Store's own handles for Complete/Reschedule/Park to settle this exact
// claim (guarding against a second worker claiming the same item).
type ClaimedItem struct {
	QueueItem
	ID       string
	ClaimID  string
	Attempts int // prior transient-failure count, for Params.MaxAttempts
}

// Store is CrawlManager's own bookkeeping persistence: cursors, index
// cadence, and the work queue. A plain injected interface, not a plugin
// surface -- Postgres is the only implementation anticipated right now.
type Store interface {
	// GetCatalogCursor returns catalogID's stored cursor, or seen=false if
	// this catalog has never been synced. The write side is Complete, which
	// advances the cursor and settles the queue claim together -- there is
	// deliberately no separate cursor-only write method, so a caller can't
	// advance one without the other and drift them apart.
	GetCatalogCursor(ctx context.Context, catalogID string) (cursor CatalogCursor, seen bool, err error)
	RecordFailure(ctx context.Context, report PassReport) error

	// GetCatalogEnvelope returns the descriptor/provider/catalogType/
	// participantId captured from catalogID's last successful sync, or
	// ok=false if it was never captured (never synced, or synced without
	// ever carrying one). Retire uses this to build a Discovery wipe push
	// once the index entry itself no longer carries any content to fetch.
	GetCatalogEnvelope(ctx context.Context, catalogID string) (descriptor, provider json.RawMessage, catalogType, participantID string, ok bool, err error)

	// GetIndexCursor returns indexURL's stored cursor, or nil if this index
	// has never been polled.
	GetIndexCursor(ctx context.Context, indexURL string) (*IndexCursor, error)
	UpsertIndexCursor(ctx context.Context, cursor IndexCursor) error

	Enqueue(ctx context.Context, item QueueItem) error
	// ClaimNext claims and returns the next queued item, or nil if the
	// queue is empty.
	ClaimNext(ctx context.Context) (*ClaimedItem, error)
	Complete(ctx context.Context, id, claimID string, cursor CatalogCursor) error
	Reschedule(ctx context.Context, id, claimID string, nextAttemptAt time.Time) error
	Park(ctx context.Context, id, claimID string) error
}
