package client

import (
	"context"
	"encoding/json"
	"fmt"

	openfga "github.com/openfga/go-sdk"
	fgaclient "github.com/openfga/go-sdk/client"
)

// fgaClient is the concrete FGAClient backed by the OpenFGA Go SDK.
type fgaClient struct {
	inner *fgaclient.OpenFgaClient
}

// Config holds connection settings for the OpenFGA server.
type Config struct {
	APIURL  string // e.g. "http://localhost:8080"
	StoreID string // default store; can be overridden per-request
}

// New creates an FGAClient connected to the given OpenFGA server.
func New(cfg Config) (FGAClient, error) {
	c, err := fgaclient.NewSdkClient(&fgaclient.ClientConfiguration{
		ApiUrl:  cfg.APIURL,
		StoreId: cfg.StoreID,
	})
	if err != nil {
		return nil, fmt.Errorf("client.New: %w", err)
	}
	return &fgaClient{inner: c}, nil
}

// Check calls the OpenFGA Check endpoint.
func (c *fgaClient) Check(ctx context.Context, req CheckRequest) (bool, error) {
	body := fgaclient.ClientCheckRequest{
		User:     req.User,
		Relation: req.Relation,
		Object:   req.Object,
	}
	opts := &fgaclient.ClientCheckOptions{}
	if req.AuthorizationModelID != "" {
		opts.AuthorizationModelId = &req.AuthorizationModelID
	}

	resp, err := c.inner.Check(ctx).Body(body).Options(*opts).Execute()
	if err != nil {
		return false, fmt.Errorf("client.Check: %w", err)
	}
	return resp.GetAllowed(), nil
}

// ListObjects calls the OpenFGA ListObjects endpoint.
func (c *fgaClient) ListObjects(ctx context.Context, req ListObjectsRequest) ([]string, error) {
	body := fgaclient.ClientListObjectsRequest{
		User:     req.User,
		Relation: req.Relation,
		Type:     req.Type,
	}
	opts := &fgaclient.ClientListObjectsOptions{}
	if req.AuthorizationModelID != "" {
		opts.AuthorizationModelId = &req.AuthorizationModelID
	}

	resp, err := c.inner.ListObjects(ctx).Body(body).Options(*opts).Execute()
	if err != nil {
		return nil, fmt.Errorf("client.ListObjects: %w", err)
	}
	return resp.GetObjects(), nil
}

// ReadTuples reads all tuples in the store by paging through Read responses.
func (c *fgaClient) ReadTuples(ctx context.Context, _ string) ([]Tuple, error) {
	var all []Tuple
	var token string
	for {
		opts := fgaclient.ClientReadOptions{}
		if token != "" {
			opts.ContinuationToken = &token
		}
		resp, err := c.inner.Read(ctx).Options(opts).Execute()
		if err != nil {
			return nil, fmt.Errorf("client.ReadTuples: %w", err)
		}
		for _, t := range resp.GetTuples() {
			tk := t.GetKey()
			all = append(all, Tuple{
				User:     tk.GetUser(),
				Relation: tk.GetRelation(),
				Object:   tk.GetObject(),
			})
		}
		token = resp.GetContinuationToken()
		if token == "" {
			break
		}
	}
	return all, nil
}

// WriteAuthorizationModel writes a new authorization model to OpenFGA and
// returns the assigned model ID.
func (c *fgaClient) WriteAuthorizationModel(ctx context.Context, _ string, modelDef []byte) (string, error) {
	var body openfga.WriteAuthorizationModelRequest
	if err := json.Unmarshal(modelDef, &body); err != nil {
		return "", fmt.Errorf("client.WriteAuthorizationModel: unmarshal: %w", err)
	}
	resp, err := c.inner.WriteAuthorizationModel(ctx).Body(body).Execute()
	if err != nil {
		return "", fmt.Errorf("client.WriteAuthorizationModel: %w", err)
	}
	return resp.GetAuthorizationModelId(), nil
}

// ReadChanges polls the OpenFGA ReadChanges endpoint and converts the
// response to the library's TupleChange type.
func (c *fgaClient) ReadChanges(ctx context.Context, storeID, continuationToken string) ([]TupleChange, string, error) {
	opts := fgaclient.ClientReadChangesOptions{}
	if continuationToken != "" {
		opts.ContinuationToken = &continuationToken
	}

	resp, err := c.inner.ReadChanges(ctx).Options(opts).Execute()
	if err != nil {
		return nil, "", fmt.Errorf("client.ReadChanges: %w", err)
	}

	changes := make([]TupleChange, 0, len(resp.GetChanges()))
	for _, ch := range resp.GetChanges() {
		op := OperationWrite
		if ch.GetOperation() == openfga.TUPLEOPERATION_DELETE {
			op = OperationDelete
		}
		tk := ch.GetTupleKey()
		changes = append(changes, TupleChange{
			User:      tk.GetUser(),
			Relation:  tk.GetRelation(),
			Object:    tk.GetObject(),
			Operation: op,
		})
	}

	return changes, resp.GetContinuationToken(), nil
}
