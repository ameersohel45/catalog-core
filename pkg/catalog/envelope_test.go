package catalog

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestExtractEnvelope_ReadsDescriptorAndProvider(t *testing.T) {
	doc := []byte(`{"id":"p/c","descriptor":{"name":"Test"},"provider":{"id":"p"},"resources":[]}`)
	descriptor, provider, err := ExtractEnvelope(doc)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(descriptor)) != `{"name":"Test"}` {
		t.Fatalf("descriptor = %s, want the descriptor object", descriptor)
	}
	if strings.TrimSpace(string(provider)) != `{"id":"p"}` {
		t.Fatalf("provider = %s, want the provider object", provider)
	}
}

func TestBuildRetireDoc_HasNoResourcesAndEmptyArray(t *testing.T) {
	doc, err := BuildRetireDoc("p/c", json.RawMessage(`{"name":"Test"}`), json.RawMessage(`{"id":"p"}`))
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(doc, &got); err != nil {
		t.Fatal(err)
	}
	if strings.Trim(string(got["id"]), `"`) != "p/c" {
		t.Fatalf("id = %s, want p/c", got["id"])
	}
	if strings.TrimSpace(string(got["resources"])) != "[]" {
		t.Fatalf("resources = %s, want an explicit empty array", got["resources"])
	}
	if len(got["descriptor"]) == 0 || len(got["provider"]) == 0 {
		t.Fatalf("got = %+v, want descriptor and provider carried through", got)
	}
}

func TestBuildRetireDoc_OmitsMissingEnvelopeFields(t *testing.T) {
	doc, err := BuildRetireDoc("p/c", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(doc, &got); err != nil {
		t.Fatal(err)
	}
	if _, ok := got["descriptor"]; ok {
		t.Fatalf("got = %+v, want no descriptor key when none was captured", got)
	}
}
