package lister

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/nikosubra/emptybucket-portable/logger"
)

// StartProducer scans the bucket and sends delete batches over the provided channel.
// It runs in a goroutine and closes the channel when complete.
func StartProducer(
	ctx context.Context,
	client *s3.Client,
	bucket string,
	batchSize int,
	batchChan chan<- []types.ObjectIdentifier,
	logInfo func(string, ...interface{}),
	logError func(string, ...interface{}),
	versioningEnabled bool,
) {
	go func() {
		defer close(batchChan)

		var currentBatch []types.ObjectIdentifier
		paginator := s3.NewListObjectVersionsPaginator(client, &s3.ListObjectVersionsInput{
			Bucket: aws.String(bucket),
		})

		for paginator.HasMorePages() {
			ctxPage, cancel := context.WithTimeout(ctx, 60*time.Second)
			page, err := paginator.NextPage(ctxPage)
			cancel()
			if err != nil {
				logError("Error listing objects: %v", err)
				return
			}

			logInfo("S3 page read completed: %d versions and %d delete markers", len(page.Versions), len(page.DeleteMarkers))
			if len(page.Versions) == 0 && len(page.DeleteMarkers) == 0 {
				logInfo("No objects returned in this page — the bucket may be empty or fully processed.")
			}

			for _, v := range page.Versions {
				key := aws.ToString(v.Key)
				ver := aws.ToString(v.VersionId)
				logger.Debug("Checking object: %s", key)
				logInfo("Queueing object: %s version: %s", key, ver)
				currentBatch = append(currentBatch, types.ObjectIdentifier{
					Key: v.Key, VersionId: v.VersionId,
				})
				if len(currentBatch) >= batchSize {
					batchChan <- currentBatch
					logInfo("🔄 Batch sent with %d objects", len(currentBatch))
					currentBatch = nil
				}
			}

			for _, dm := range page.DeleteMarkers {
				key := aws.ToString(dm.Key)
				ver := aws.ToString(dm.VersionId)
				logger.Debug("Checking object: %s", key)
				logInfo("Queueing object: %s version: %s", key, ver)
				currentBatch = append(currentBatch, types.ObjectIdentifier{
					Key: dm.Key, VersionId: dm.VersionId,
				})
				if len(currentBatch) >= batchSize {
					batchChan <- currentBatch
					logInfo("🔄 Batch sent with %d objects", len(currentBatch))
					currentBatch = nil
				}
			}
		}

		if len(currentBatch) > 0 {
			batchChan <- currentBatch
			logInfo("🔄 Final batch sent with %d objects", len(currentBatch))
		}
	}()
}
