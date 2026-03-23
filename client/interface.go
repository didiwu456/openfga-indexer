// Package client wraps the OpenFGA Go SDK and exposes a minimal interface
// used by the rest of this library (builder, watcher, check).
package client

import "context"

// Tuple is a single (user, relation, object) relationship record.
type Tuple struct {
	User     string
	Relation string
	Object   string
}

// CheckRequest parameters for a single authorization check.
type CheckRequest struct {
	StoreID              string
	AuthorizationModelID string // optional; uses latest if empty
	User                 string
	Relation             string
	Object               string
}

// ListObjectsRequest parameters for listing objects a user has a relation to.
type ListObjectsRequest struct {
	StoreID              string
	AuthorizationModelID string
	User                 string
	Relation             string
	Type                 string // object type to list (e.g. "document")
}

// TupleChange represents a single change event from ReadChanges.
type TupleChange struct {
	User      string
	Relation  string
	Object    string
	Operation Operation
}

// Operation is the type of tuple change.
type Operation int

const (
	OperationWrite  Operation = iota // tuple was added
	OperationDelete                  // tuple was removed
)

// FGAClient is the minimal interface the library needs from OpenFGA.
// The concrete implementation wraps github.com/openfga/go-sdk/client.
type FGAClient interface {
	// Check returns true if the user has the given relation to the object.
	Check(ctx context.Context, req CheckRequest) (bool, error)

	// ListObjects returns all objects of req.Type that req.User has req.Relation to.
	ListObjects(ctx context.Context, req ListObjectsRequest) ([]string, error)

	// ReadTuples returns all tuples in the store, paging through continuation
	// tokens automatically. Used by the offline builder for full snapshots.
	ReadTuples(ctx context.Context, storeID string) ([]Tuple, error)

	// ReadChanges streams tuple changes since continuationToken.
	// Returns the changes, the next continuation token, and any error.
	// Pass an empty token to start from the beginning.
	ReadChanges(ctx context.Context, storeID, continuationToken string) ([]TupleChange, string, error)
}
