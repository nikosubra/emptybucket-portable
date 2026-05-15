package lister

import (
	"context"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3API is the slice of the S3 client surface that the lister actually uses.
// Defining it as an interface lets tests pass a fake without spinning up a
// real S3 endpoint.
type S3API interface {
	ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	ListObjectVersions(ctx context.Context, in *s3.ListObjectVersionsInput, opts ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error)
}

type Inventory struct {
	TotalObjects     int64
	TopLevelFolders  int
	TotalSizeBytes   int64
	VersionedObjects int64
	DeleteMarkers    int64
	Elapsed          time.Duration
}

// Scan walks the bucket once (twice if versioned) and tallies totals. When
// prefix is non-empty, only matching keys are counted.
func Scan(ctx context.Context, client S3API, bucket, prefix string, versioned bool) (*Inventory, error) {
	start := time.Now()
	inv := &Inventory{}
	topLevel := make(map[string]struct{})

	var continuationToken *string
	for {
		ctxPage, cancel := context.WithTimeout(ctx, 120*time.Second)
		out, err := client.ListObjectsV2(ctxPage, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            stringOrNil(prefix),
			ContinuationToken: continuationToken,
		})
		cancel()
		if err != nil {
			return nil, err
		}
		for _, obj := range out.Contents {
			inv.TotalObjects++
			inv.TotalSizeBytes += aws.ToInt64(obj.Size)
			key := aws.ToString(obj.Key)
			if seg := topLevelSegment(key, prefix); seg != "" {
				topLevel[seg] = struct{}{}
			}
		}
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		continuationToken = out.NextContinuationToken
	}
	inv.TopLevelFolders = len(topLevel)

	if versioned {
		var keyMarker, versionIdMarker *string
		for {
			ctxPage, cancel := context.WithTimeout(ctx, 120*time.Second)
			out, err := client.ListObjectVersions(ctxPage, &s3.ListObjectVersionsInput{
				Bucket:          aws.String(bucket),
				Prefix:          stringOrNil(prefix),
				KeyMarker:       keyMarker,
				VersionIdMarker: versionIdMarker,
			})
			cancel()
			if err != nil {
				return nil, err
			}
			inv.VersionedObjects += int64(len(out.Versions))
			inv.DeleteMarkers += int64(len(out.DeleteMarkers))
			if !aws.ToBool(out.IsTruncated) {
				break
			}
			keyMarker = out.NextKeyMarker
			versionIdMarker = out.NextVersionIdMarker
		}
	}

	inv.Elapsed = time.Since(start)
	return inv, nil
}

// topLevelSegment returns the first path component after prefix. Returns ""
// when the key has no further path separator (i.e. the key is itself at the
// "root" of the prefix view).
func topLevelSegment(key, prefix string) string {
	rest := strings.TrimPrefix(key, prefix)
	if i := strings.Index(rest, "/"); i > 0 {
		return rest[:i]
	}
	return ""
}

func stringOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return aws.String(s)
}
