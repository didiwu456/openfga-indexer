// Package index implements an in-memory Leopard-style index for fast group
// membership resolution without recursive OpenFGA API calls.
//
// It maintains two posting-list maps:
//
//	GROUP2GROUP[g] = set of all groups that are descendants of g (transitive)
//	MEMBER2GROUP[u] = set of all groups that directly contain user u
//
// Membership test: user U is a member of group G iff
//
//	MEMBER2GROUP(U) ∩ GROUP2GROUP(G) ≠ ∅
package index

// Index is the public interface for the Leopard in-memory index.
// Implementations must be safe for concurrent use.
type Index interface {
	// ApplyTupleWrite records a new (user, relation, object) tuple.
	// relation should be a group-membership relation (e.g. "member").
	ApplyTupleWrite(user, relation, object string)

	// ApplyTupleDelete removes an existing (user, relation, object) tuple.
	ApplyTupleDelete(user, relation, object string)

	// IsMember returns true if userID transitively belongs to groupID.
	IsMember(userID, groupID string) bool

	// GroupsForUser returns all groups the user directly belongs to.
	GroupsForUser(userID string) []string

	// DescendantGroups returns all groups that are descendants of groupID
	// (including groupID itself).
	DescendantGroups(groupID string) []string

	// Snapshot returns a point-in-time copy of the raw maps for debugging.
	Snapshot() Snapshot
}

// Snapshot is a read-only view of the index state at a point in time.
type Snapshot struct {
	// MemberToGroup maps userID → sorted list of direct parent group IDs.
	MemberToGroup map[string][]string
	// GroupToGroup maps groupID → sorted list of all descendant group IDs
	// (including self).
	GroupToGroup map[string][]string
}
