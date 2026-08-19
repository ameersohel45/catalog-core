package catalog

// fetch_test.go — covers Fetcher end to end against httptest servers, using
// artifactsigner.SignJSON to sign fixtures exactly as catalog/publisher does:
// index fetch with per-entry signature verification (and dropping a forged
// entry), file fetch with digest + signature + envelope-unwrap for both a
// CatalogFileDoc baseline and a ChangeFileDoc, the CON-TBD-12 cross-check,
// the Canonicalization guard, and conditional-GET (200-then-304).

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/beckn/catalog-core/pkg/crawler"
	"github.com/beckn/catalog-core/pkg/security/artifactsigner"
)

const testKeyID = "key-1"

func newTestKeypair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	return pub, priv
}

func testKeys(pub ed25519.PublicKey) crawler.KeySource {
	return crawler.StaticKeys(map[string]ed25519.PublicKey{testKeyID: pub})
}

// signEntry mirrors catalog/publisher's signEntry: marshal, sign with
// "signature" excluded, attach, re-marshal.
func signEntry(t *testing.T, priv ed25519.PrivateKey, entry CatalogEntry) []byte {
	t.Helper()
	unsigned, err := json.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	sigValue, err := artifactsigner.SignJSON(unsigned, "signature", priv)
	if err != nil {
		t.Fatal(err)
	}
	entry.Signature = EntrySignature{KeyID: testKeyID, Value: sigValue}
	out, err := json.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// signBaseline mirrors catalog/publisher's signCatalogFile.
func signBaseline(t *testing.T, priv ed25519.PrivateKey, canon string, doc CatalogFileDoc) []byte {
	t.Helper()
	unsigned, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	sigValue, err := artifactsigner.SignJSON(unsigned, "signature", priv)
	if err != nil {
		t.Fatal(err)
	}
	doc.Signature = FileSignature{KeyID: testKeyID, Canonicalization: canon, Value: sigValue}
	out, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// signChange mirrors catalog/publisher's signChangeFile.
func signChange(t *testing.T, priv ed25519.PrivateKey, doc ChangeFileDoc) []byte {
	t.Helper()
	unsigned, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	sigValue, err := artifactsigner.SignJSON(unsigned, "signature", priv)
	if err != nil {
		t.Fatal(err)
	}
	doc.Signature = FileSignature{KeyID: testKeyID, Canonicalization: "JCS", Value: sigValue}
	out, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func sha256Prefixed(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha-256:" + hex.EncodeToString(sum[:])
}

func newFetcher(pub ed25519.PublicKey) *Fetcher {
	client := crawler.NewClient(5*time.Second, 1<<20, true) // allowPrivate: httptest is 127.0.0.1
	return NewFetcher(client, testKeys(pub), 1<<20)
}

func serveBody(t *testing.T, body []byte) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestFetchIndex_VerifiesAndParsesEntries(t *testing.T) {
	pub, priv := newTestKeypair(t)
	entryJSON := signEntry(t, priv, CatalogEntry{
		CatalogID:   "p/c",
		CatalogType: "REGULAR",
		Baseline:    FileEntry{Version: 1, URL: "https://x/b.json", Size: 10, Digest: "sha-256:abc"},
	})
	index, err := json.Marshal(rawIndex{NodeID: "node-1", NextUpdate: "2026-01-01T00:00:00Z", Catalogs: []json.RawMessage{entryJSON}})
	if err != nil {
		t.Fatal(err)
	}
	srv := serveBody(t, index)

	f := newFetcher(pub)
	res, err := f.FetchIndex(context.Background(), srv.URL, IndexConditions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Index.NodeID != "node-1" {
		t.Fatalf("nodeId = %q, want node-1", res.Index.NodeID)
	}
	if len(res.Index.Catalogs) != 1 || res.Index.Catalogs[0].CatalogID != "p/c" {
		t.Fatalf("catalogs = %+v, want one entry p/c", res.Index.Catalogs)
	}
	if len(res.Dropped) != 0 {
		t.Fatalf("dropped = %+v, want none", res.Dropped)
	}
}

func TestFetchIndex_DropsUnverifiedEntry(t *testing.T) {
	pub, priv := newTestKeypair(t)
	good := signEntry(t, priv, CatalogEntry{
		CatalogID: "p/good", CatalogType: "REGULAR",
		Baseline: FileEntry{Version: 1, URL: "https://x/b.json", Size: 1, Digest: "sha-256:abc"},
	})
	forged, err := json.Marshal(CatalogEntry{
		CatalogID: "p/bad", CatalogType: "REGULAR",
		Baseline:  FileEntry{Version: 1, URL: "https://x/b.json", Size: 1, Digest: "sha-256:abc"},
		Signature: EntrySignature{KeyID: testKeyID, Value: base64.StdEncoding.EncodeToString([]byte("not-a-real-signature-000000000"))},
	})
	if err != nil {
		t.Fatal(err)
	}
	index, err := json.Marshal(rawIndex{NodeID: "node-1", Catalogs: []json.RawMessage{good, forged}})
	if err != nil {
		t.Fatal(err)
	}
	srv := serveBody(t, index)

	f := newFetcher(pub)
	res, err := f.FetchIndex(context.Background(), srv.URL, IndexConditions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Index.Catalogs) != 1 || res.Index.Catalogs[0].CatalogID != "p/good" {
		t.Fatalf("catalogs = %+v, want only p/good", res.Index.Catalogs)
	}
	if len(res.Dropped) != 1 || res.Dropped[0].CatalogID != "p/bad" {
		t.Fatalf("dropped = %+v, want p/bad", res.Dropped)
	}
}

func TestFetchIndex_ConditionalGet_NotModified(t *testing.T) {
	pub, _ := newTestKeypair(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		w.Write([]byte(`{"nodeId":"node-1","catalogs":[]}`))
	}))
	defer srv.Close()

	f := newFetcher(pub)
	ctx := context.Background()
	first, err := f.FetchIndex(ctx, srv.URL, IndexConditions{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.FetchIndex(ctx, srv.URL, IndexConditions{ETag: first.ETag})
	if err != nil {
		t.Fatal(err)
	}
	if !second.NotModified {
		t.Fatalf("second = %+v, want NotModified", second)
	}
}

func TestFetchFile_BaselineVerifiedAndUnwrapped(t *testing.T) {
	pub, priv := newTestKeypair(t)
	catalogContent := json.RawMessage(`{"id":"p/c","resources":[]}`)
	baseline := signBaseline(t, priv, "JCS", CatalogFileDoc{CatalogID: "p/c", Version: 1, NextUpdate: time.Now(), Catalog: catalogContent})
	srv := serveBody(t, baseline)

	f := newFetcher(pub)
	entry := FileEntry{URL: srv.URL, Version: 1, Digest: sha256Prefixed(baseline)}
	got, err := f.FetchFile(context.Background(), "node-1", "p/c", entry)
	if err != nil {
		t.Fatal(err)
	}
	var unwrapped map[string]any
	if err := json.Unmarshal(got, &unwrapped); err != nil {
		t.Fatal(err)
	}
	if unwrapped["id"] != "p/c" {
		t.Fatalf("unwrapped = %+v, want id p/c (envelope should be stripped)", unwrapped)
	}
}

func TestFetchFile_ChangeFileVerifiedAndUnchanged(t *testing.T) {
	pub, priv := newTestKeypair(t)
	change := signChange(t, priv, ChangeFileDoc{CatalogID: "p/c", FromVersion: 1, ToVersion: 2, NextUpdate: time.Now()})
	srv := serveBody(t, change)

	f := newFetcher(pub)
	entry := FileEntry{URL: srv.URL, FromVersion: 1, ToVersion: 2, Digest: sha256Prefixed(change)}
	got, err := f.FetchFile(context.Background(), "node-1", "p/c", entry)
	if err != nil {
		t.Fatal(err)
	}
	var doc ChangeFileDoc
	if err := json.Unmarshal(got, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.CatalogID != "p/c" || doc.ToVersion != 2 {
		t.Fatalf("doc = %+v, want catalogId p/c toVersion 2", doc)
	}
}

func TestFetchFile_DigestMismatchIsPermanent(t *testing.T) {
	pub, priv := newTestKeypair(t)
	baseline := signBaseline(t, priv, "JCS", CatalogFileDoc{CatalogID: "p/c", Version: 1, NextUpdate: time.Now(), Catalog: json.RawMessage(`{"id":"p/c"}`)})
	srv := serveBody(t, baseline)

	f := newFetcher(pub)
	entry := FileEntry{URL: srv.URL, Version: 1, Digest: "sha-256:deadbeef"}
	_, err := f.FetchFile(context.Background(), "node-1", "p/c", entry)
	if err == nil || !crawler.IsPermanent(err) {
		t.Fatalf("expected a permanent digest-mismatch error, got %v", err)
	}
	if crawler.PermanentClass(err) != crawler.FaultDigestMismatch {
		t.Fatalf("class = %q, want %q", crawler.PermanentClass(err), crawler.FaultDigestMismatch)
	}
}

// CON-TBD-12: a correctly signed file whose own declared version doesn't
// match what the index entry expected must be rejected, not silently
// accepted as if it were the referenced content.
func TestFetchFile_VersionMismatchIsRejected(t *testing.T) {
	pub, priv := newTestKeypair(t)
	baseline := signBaseline(t, priv, "JCS", CatalogFileDoc{CatalogID: "p/c", Version: 2, NextUpdate: time.Now(), Catalog: json.RawMessage(`{"id":"p/c"}`)}) // signed as version 2
	srv := serveBody(t, baseline)

	f := newFetcher(pub)
	entry := FileEntry{URL: srv.URL, Version: 1, Digest: sha256Prefixed(baseline)} // index says version 1
	_, err := f.FetchFile(context.Background(), "node-1", "p/c", entry)
	if err == nil || !crawler.IsPermanent(err) {
		t.Fatalf("expected a permanent error, got %v", err)
	}
	if crawler.PermanentClass(err) != crawler.FaultDigestMismatch {
		t.Fatalf("class = %q, want %q (content-identity mismatch, not a signature failure)", crawler.PermanentClass(err), crawler.FaultDigestMismatch)
	}
}

func TestFetchFile_UnsupportedCanonicalizationIsRejected(t *testing.T) {
	pub, priv := newTestKeypair(t)
	baseline := signBaseline(t, priv, "RFC8785-STRICT", CatalogFileDoc{CatalogID: "p/c", Version: 1, NextUpdate: time.Now(), Catalog: json.RawMessage(`{"id":"p/c"}`)})
	srv := serveBody(t, baseline)

	f := newFetcher(pub)
	entry := FileEntry{URL: srv.URL, Version: 1, Digest: sha256Prefixed(baseline)}
	_, err := f.FetchFile(context.Background(), "node-1", "p/c", entry)
	if err == nil || !crawler.IsPermanent(err) {
		t.Fatalf("expected a permanent error, got %v", err)
	}
	if crawler.PermanentClass(err) != crawler.FaultSignature {
		t.Fatalf("class = %q, want %q (an unsupported scheme is a signature-gate rejection, not a content problem)", crawler.PermanentClass(err), crawler.FaultSignature)
	}
}

func TestFetchFile_UnsignedIsRejected(t *testing.T) {
	unsigned, err := json.Marshal(CatalogFileDoc{CatalogID: "p/c", Version: 1, Catalog: json.RawMessage(`{"id":"p/c"}`)})
	if err != nil {
		t.Fatal(err)
	}
	srv := serveBody(t, unsigned)

	pub, _ := newTestKeypair(t)
	f := newFetcher(pub)
	entry := FileEntry{URL: srv.URL, Version: 1, Digest: sha256Prefixed(unsigned)}
	_, err = f.FetchFile(context.Background(), "node-1", "p/c", entry)
	if err == nil || !crawler.IsPermanent(err) {
		t.Fatalf("expected a permanent signature error, got %v", err)
	}
}
