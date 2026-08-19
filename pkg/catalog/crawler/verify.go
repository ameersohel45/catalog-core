package crawler

// verify.go — resolving a signing key (via a caller-supplied KeySource), and
// verifying a self-signature over an arbitrary JSON document against that
// key.
//
// This is deliberately content-agnostic: it verifies "this document, minus
// its own named signature field, was signed by the key registered for
// (nodeID, keyID)". It has no notion of a catalog index, an entry, or a
// baseline/change envelope — a caller that fetches self-signed catalog files,
// DeDi manifests, or any other self-signed artifact can use the same gate.
// Unwrapping a caller-specific signed envelope (e.g. a catalog baseline's
// {catalog, signature} wrapper) and cross-checking fields the caller's own
// format declares is the caller's job, layered on top of VerifySignature.
//
// This package makes no registry call itself and knows nothing about a
// network registry's record shape or caching policy -- a caller wires a
// KeySource however it resolves keys (a registry lookup, a cache in front of
// one, a static map), and this package only ever consumes the resulting key.
//
// The gate fails CLOSED. No key source, no signature, an unknown keyId, or
// undecodable key material all reject the document.

import (
	"context"
	"crypto/ed25519"
	"fmt"

	"github.com/beckn/catalog-core/pkg/security/artifactverifier"
)

// KeySource resolves the public Ed25519 key a node signed with, given the
// node (domain identity) that published the content and the keyId the
// signing entity named. It is injected rather than derived, so this layer
// never trusts a key carried by the same untrusted content it is checking.
//
// An error means "this cannot be verified", and the error's class decides a
// caller's retry-vs-give-up decision:
//   - a PermanentError of class FaultSignature is a definitive verdict (no
//     such key, revoked key, unusable key material) and should not be retried;
//   - any other error is transient (the registry could not be reached) and
//     may be retried.
//
// An unknown keyId must return an error, never a zero key.
type KeySource func(ctx context.Context, nodeID, keyID string) (ed25519.PublicKey, error)

// StaticKeys adapts an already-resolved keyId -> public key map to a
// KeySource, ignoring the node.
//
// TEST HELPER ONLY. A production KeySource resolves keys through a caller's
// own registry client, never from deployment config.
func StaticKeys(keys map[string]ed25519.PublicKey) KeySource {
	return func(_ context.Context, _, keyID string) (ed25519.PublicKey, error) {
		pub, ok := keys[keyID]
		if !ok {
			return nil, PermanentFaultf(FaultSignature, "crawler: keyId %q is not a known key", keyID)
		}
		return pub, nil
	}
}

// ResolveSigningKey is the shared "look up keyID under nodeID, give a
// definitive verdict or a retryable one" step a caller's own signature gate
// uses before verifying.
func ResolveSigningKey(ctx context.Context, keys KeySource, nodeID, keyID, what string) (ed25519.PublicKey, error) {
	if keys == nil {
		// Fail closed: with no key source there is nothing to verify against, so
		// the signature gate would otherwise silently degrade to "unverified".
		return nil, PermanentFaultf(FaultSignature, "crawler: no key source configured, refusing unverifiable %s", what)
	}
	pub, err := keys(ctx, nodeID, keyID)
	if err != nil {
		if IsPermanent(err) {
			return nil, PermanentFaultf(FaultSignature, "crawler: %s resolving signing key %q: %v", what, keyID, err)
		}
		// The key source could not answer (registry unreachable). Stay transient
		// so a caller retries instead of giving up on healthy content.
		return nil, fmt.Errorf("crawler: %s resolving signing key %q: %w", what, keyID, err)
	}
	if len(pub) != ed25519.PublicKeySize {
		return nil, PermanentFaultf(FaultSignature, "crawler: %s key %q is not a %d-byte Ed25519 key", what, keyID, ed25519.PublicKeySize)
	}
	return pub, nil
}

// VerifySignature verifies a self-signature over raw: the document minus its
// own field named sigField was signed, under keyID, by the key registered for
// nodeID. This is the generic gate -- it does not know or care what raw
// otherwise contains, so unwrapping a caller-specific envelope or
// cross-checking caller-specific fields (e.g. a declared id/version matching
// what an index entry expected) is the caller's responsibility, done after
// this call succeeds.
func VerifySignature(ctx context.Context, keys KeySource, nodeID, keyID, sigValue string, raw []byte, sigField string) error {
	if sigValue == "" || keyID == "" {
		return PermanentFaultf(FaultSignature, "crawler: no signature to verify")
	}
	pub, err := ResolveSigningKey(ctx, keys, nodeID, keyID, "self-signature")
	if err != nil {
		return err
	}
	if err := artifactverifier.VerifyJSON(raw, sigField, sigValue, pub); err != nil {
		return PermanentFaultf(FaultSignature, "crawler: signature verification failed: %v", err)
	}
	return nil
}
