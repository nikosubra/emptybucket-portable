package lister

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/nikosubra/emptybucket-portable/logger"
)

// StartProducer scans the bucket and sends delete batches over the provided
// channel. It runs in a goroutine and closes the channel when complete. When
// prefix is non-empty, only matching keys are enqueued. For versioned buckets
// it uses ListObjectVersions so every version and delete marker is included.
func StartProducer(
	ctx context.Context,
	client S3API,
	bucket, prefix string,
	batchSize int,
	batchChan chan<- []types.ObjectIdentifier,
	logInfo func(string, ...interface{}),
	logError func(string, ...interface{}),
	versioningEnabled bool,
) {
	go func() {
		defer close(batchChan)
		if versioningEnabled {
			produceVersioned(ctx, client, bucket, prefix, batchSize, batchChan, logInfo, logError)
		} else {
			produceFlat(ctx, client, bucket, prefix, batchSize, batchChan, logInfo, logError)
		}
	}()
}

func produceFlat(
	ctx context.Context,
	client S3API,
	bucket, prefix string,
	batchSize int,
	batchChan chan<- []types.ObjectIdentifier,
	logInfo func(string, ...interface{}),
	logError func(string, ...interface{}),
) {
	logInfo("StartProducer (flat) for bucket: %s prefix=%q — ListObjectsV2", bucket, prefix)
	var totalCount int
	var currentBatch []types.ObjectIdentifier
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
			logError("ListObjectsV2 failed: %v", err)
			return
		}
		for _, obj := range out.Contents {
			logger.Debug("Queueing object: %s", aws.ToString(obj.Key))
			currentBatch = append(currentBatch, types.ObjectIdentifier{Key: obj.Key})
			totalCount++
			if len(currentBatch) >= batchSize {
				batchChan <- currentBatch
				currentBatch = nil
			}
		}
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		continuationToken = out.NextContinuationToken
	}

	if len(currentBatch) > 0 {
		batchChan <- currentBatch
	}
	logInfo("Listing complete. Queued %d objects.", totalCount)
}

func produceVersioned(
	ctx context.Context,
	client S3API,
	bucket, prefix string,
	batchSize int,
	batchChan chan<- []types.ObjectIdentifier,
	logInfo func(string, ...interface{}),
	logError func(string, ...interface{}),
) {
	logInfo("StartProducer (versioned) for bucket: %s prefix=%q — ListObjectVersions", bucket, prefix)
	var totalCount int
	var currentBatch []types.ObjectIdentifier
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
			logError("ListObjectVersions failed: %v", err)
			return
		}
		for _, v := range out.Versions {
			currentBatch = append(currentBatch, types.ObjectIdentifier{Key: v.Key, VersionId: v.VersionId})
			totalCount++
			if len(currentBatch) >= batchSize {
				batchChan <- currentBatch
				currentBatch = nil
			}
		}
		for _, dm := range out.DeleteMarkers {
			currentBatch = append(currentBatch, types.ObjectIdentifier{Key: dm.Key, VersionId: dm.VersionId})
			totalCount++
			if len(currentBatch) >= batchSize {
				batchChan <- currentBatch
				currentBatch = nil
			}
		}
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		keyMarker = out.NextKeyMarker
		versionIdMarker = out.NextVersionIdMarker
	}

	if len(currentBatch) > 0 {
		batchChan <- currentBatch
	}
	logInfo("Versioned listing complete. Queued %d entries.", totalCount)
}
