package catalog

// identity.go — StampIdentity backstops a resolved catalog's bppId/bppUri
// (the same transaction-leg identity fields a push's context already
// carries, included on the catalog document itself too so a downstream
// consumer that unwraps just .catalog back onto the wire still has them)
// when the publisher's own content didn't already supply them. Ported from
// the catalog-crawler prototype's own StampBppIdentity.

import (
	"encoding/json"
	"fmt"
)

// StampIdentity sets doc's bppId/bppUri fields to bppID/bppURI, but only
// when doc doesn't already carry that field -- bppId/bppUri are
// schema-optional, so a publisher that DID write them is not corrected or
// second-guessed here, only backstopped when genuinely absent. An empty
// bppID or bppURI is simply not stamped (rather than writing a blank value
// over "unset") -- bppURI in particular may be empty when a caller couldn't
// resolve it (e.g. a registry lookup failure).
func StampIdentity(doc []byte, bppID, bppURI string) ([]byte, error) {
	if bppID == "" && bppURI == "" {
		return doc, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(doc, &fields); err != nil {
		return nil, fmt.Errorf("catalog: reading catalog to stamp identity: %w", err)
	}
	if _, ok := fields["bppId"]; !ok && bppID != "" {
		b, err := json.Marshal(bppID)
		if err != nil {
			return nil, err
		}
		fields["bppId"] = b
	}
	if _, ok := fields["bppUri"]; !ok && bppURI != "" {
		b, err := json.Marshal(bppURI)
		if err != nil {
			return nil, err
		}
		fields["bppUri"] = b
	}
	return json.Marshal(fields)
}
