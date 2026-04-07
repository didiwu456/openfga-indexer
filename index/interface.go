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

// IndexConfig controls which tuples the index tracks.
// Zero value is valid and uses the defaults documented on each field.
type IndexConfig struct {
	// GroupRelation is the relation name that denotes group membership.
	// Default: "member".
	GroupRelation string

	// GroupPrefix is the type prefix that identifies group objects.
	// Default: "group:".
	GroupPrefix string

	// ObjectType labels this shard for multi-type deployments (e.g. "group").
	// Optional; informational only within this package.
	ObjectType string
}

// Index is the public interface for the Leopard in-memory index.
// Implementations must be safe for concurrent use.
type Index interface {
	// ApplyTupleWrite records a new (user, relation, object) tuple.
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
