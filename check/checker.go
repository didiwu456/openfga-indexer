// Package check provides index-first authorization checks backed by OpenFGA.
//
// Strategy:
//  1. If the relation is "member" and the object is a group, try the Leopard
//     index first (O(1) set intersection, no network).
//  2. Fall through to the FGA Check API for all other relations or when the
//     index cannot give a definitive answer.
package check

import (
	"context"
	"fmt"
	"strings"

	"github.com/yourorg/openfga-indexer/cache"
	"github.com/yourorg/openfga-indexer/client"
)

// Checker routes authorization requests to the Leopard cache or the FGA API.
type Checker struct {
	cache   *cache.Cache
	fga     client.FGAClient
	storeID string
	modelID string // optional; uses latest if empty
}

// New creates a Checker backed by a cache and an FGA client.
func New(ca *cache.Cache, fga client.FGAClient, storeID, modelID string) *Checker {
	return &Checker{cache: ca, fga: fga, storeID: storeID, modelID: modelID}
}

// Check returns true if user has relation to object.
//
// For group-membership checks it consults the in-memory Leopard index first
// and avoids a network round-trip.  All other checks are forwarded to FGA.
func (c *Checker) Check(ctx context.Context, user, relation, object string) (bool, error) {
	if relation == "member" && isGroupObject(object) {
		return c.cache.Index().IsMember(user, object), nil
	}
	allowed, err := c.fga.Check(ctx, client.CheckRequest{
		StoreID:              c.storeID,
		AuthorizationModelID: c.modelID,
		User:                 user,
		Relation:             relation,
		Object:               object,
	})
	if err != nil {
		return false, fmt.Errorf("check.Check: %w", err)
	}
	return allowed, nil
}

// ListObjects returns all objects of objectType that user has relation to.
//
// If objectType is "group" and relation is "member" the result is derived
// entirely from the Leopard index. Otherwise the FGA ListObjects API is used.
func (c *Checker) ListObjects(ctx context.Context, user, relation, objectType string) ([]string, error) {
	if relation == "member" && objectType == "group" {
		return c.cache.Index().GroupsForUser(user), nil
	}
	objs, err := c.fga.ListObjects(ctx, client.ListObjectsRequest{
		StoreID:              c.storeID,
		AuthorizationModelID: c.modelID,
		User:                 user,
		Relation:             relation,
		Type:                 objectType,
	})
	if err != nil {
		return nil, fmt.Errorf("check.ListObjects: %w", err)
	}
	return objs, nil
}

// isGroupObject returns true if object has the "group:" type prefix.
func isGroupObject(object string) bool {
	return strings.HasPrefix(object, "group:")
}
