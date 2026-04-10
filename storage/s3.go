package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Config holds settings for the S3-compatible shard store.
type S3Config struct {
	Bucket   string
	Region   string
	Endpoint string // override for MinIO or other S3-compatible stores
}

type s3Store struct {
	client *s3.Client
	bucket string
}

// NewS3Store returns a ShardStore backed by S3 (or an S3-compatible store).
func NewS3Store(cfg S3Config, awsCfg aws.Config) ShardStore {
	opts := []func(*s3.Options){}
	if cfg.Endpoint != "" {
		opts = append(opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true
		})
	}
	return &s3Store{
		client: s3.NewFromConfig(awsCfg, opts...),
		bucket: cfg.Bucket,
	}
}

func (s *s3Store) WriteShard(ctx context.Context, storeID, objectType string, epoch uint64, data []byte) error {
	key := objectKey(storeID, objectType, epoch)
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("storage.WriteShard: %w", err)
	}
	return nil
}

func (s *s3Store) ReadShard(ctx context.Context, storeID, objectType string, epoch uint64) ([]byte, error) {
	key := objectKey(storeID, objectType, epoch)
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("storage.ReadShard: %w", err)
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("storage.ReadShard: read body: %w", err)
	}
	return data, nil
}

func (s *s3Store) DeleteShard(ctx context.Context, storeID, objectType string, epoch uint64) error {
	key := objectKey(storeID, objectType, epoch)
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("storage.DeleteShard: %w", err)
	}
	return nil
}

func (s *s3Store) ListEpochs(ctx context.Context, storeID, objectType string) ([]uint64, error) {
	prefix := fmt.Sprintf("leopard/%s/%s/", storeID, objectType)
	out, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return nil, fmt.Errorf("storage.ListEpochs: %w", err)
	}
	var epochs []uint64
	for _, obj := range out.Contents {
		name := strings.TrimPrefix(*obj.Key, prefix)
		name = strings.TrimSuffix(name, ".shard")
		epoch, err := strconv.ParseUint(name, 10, 64)
		if err == nil {
			epochs = append(epochs, epoch)
		}
	}
	sort.Slice(epochs, func(i, j int) bool { return epochs[i] < epochs[j] })
	return epochs, nil
}

// DeleteOldEpochs removes all but the newest `keep` epochs for (storeID, objectType).
func DeleteOldEpochs(ctx context.Context, s ShardStore, storeID, objectType string, keep int) error {
	epochs, err := s.ListEpochs(ctx, storeID, objectType)
	if err != nil {
		return err
	}
	if len(epochs) <= keep {
		return nil
	}
	for _, epoch := range epochs[:len(epochs)-keep] {
		if err := s.DeleteShard(ctx, storeID, objectType, epoch); err != nil {
			return err
		}
	}
	return nil
}

// Reuse objectKey from memory.go — same package.
var _ = types.Object{} // ensure s3 types imported
