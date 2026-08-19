package catalog

// scope.go — network-scope resolution: decides whether a caller should carry
// a catalog at all, and the visibleTo network set to hand a destination like
// Discovery. Ported from the catalog-crawler prototype's own scope.go.

// IsPublic reports whether the catalog is visible to everyone -- no
// networkIds scope it (file spec: "empty or absent means public").
func (e CatalogEntry) IsPublic() bool { return len(e.NetworkIDs) == 0 }

// ResolveScope decides whether a caller configured for callerNetworks should
// take this catalog and, if so, the visibleTo network set to hand onward.
//
// A public catalog (no networkIds) is always taken and visible to everyone
// (visibleTo nil). A network-scoped catalog is taken only if its networks
// intersect callerNetworks, and its own networks flow through as visibleTo.
func ResolveScope(entry CatalogEntry, callerNetworks []string) (take bool, visibleTo []string) {
	if entry.IsPublic() {
		return true, nil
	}
	want := make(map[string]bool, len(callerNetworks))
	for _, n := range callerNetworks {
		want[n] = true
	}
	for _, n := range entry.NetworkIDs {
		if want[n] {
			return true, entry.NetworkIDs
		}
	}
	return false, nil
}
