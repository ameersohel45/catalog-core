package catalog

// envelope.go — the minimal envelope (id/descriptor/provider) a retirement
// needs to build a Discovery wipe push, once a retired index entry has
// dropped every file reference (baseline/changes/latest are all dropped once
// retiredAt is set) and there is nothing left to fetch content from. Operates
// on a generic JSON object rather than a typed catalog document, the same
// choice pkg/plugin/implementation/catalogcrawler/internal/sink/batch.go
// makes, so this needs no catalog-shape knowledge beyond id/descriptor/
// provider.

import (
	"encoding/json"
	"fmt"
)

// ExtractEnvelope reads a resolved catalog doc's descriptor/provider -- the
// two fields a later retire needs to build a Discovery wipe push, captured
// at every successful sync (by the caller, into its own store) rather than
// re-fetched at retire time, since a retired index entry drops every file
// reference by then.
func ExtractEnvelope(doc []byte) (descriptor, provider json.RawMessage, err error) {
	var fields struct {
		Descriptor json.RawMessage `json:"descriptor"`
		Provider   json.RawMessage `json:"provider"`
	}
	if err := json.Unmarshal(doc, &fields); err != nil {
		return nil, nil, fmt.Errorf("catalog: reading catalog to extract envelope: %w", err)
	}
	return fields.Descriptor, fields.Provider, nil
}

// BuildRetireDoc builds the Discovery wipe doc for a retired catalog: id plus
// the envelope captured from its last successful sync, with no resources/
// offers container at all -- Discovery's Catalog schema only requires id/
// descriptor/provider, so a FULL push of exactly this, naming no other
// content for catalogID, replaces the catalog's content with nothing.
//
// Resources marshals as [] rather than being omitted -- an explicitly empty
// array reads as "delete everything" unambiguously, matching Discovery's
// null-safe iterableResources.
func BuildRetireDoc(catalogID string, descriptor, provider json.RawMessage) ([]byte, error) {
	id, err := json.Marshal(catalogID)
	if err != nil {
		return nil, err
	}
	doc := map[string]json.RawMessage{
		"id":        id,
		"resources": json.RawMessage(`[]`),
	}
	if len(descriptor) > 0 {
		doc["descriptor"] = descriptor
	}
	if len(provider) > 0 {
		doc["provider"] = provider
	}
	return json.Marshal(doc)
}
