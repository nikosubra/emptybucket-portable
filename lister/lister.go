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
		logInfo("🔍 StartProducer invoked for bucket: %s — using ListObjectsV2 only", bucket)
		defer close(batchChan)

		var totalCount int

		var currentBatch []types.ObjectIdentifier
		paginatorV2 := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucket),
		})
		logInfo("🧭 Created ListObjectsV2 paginator")
		logInfo("🔎 HasMorePages: %v", paginatorV2.HasMorePages())

		for paginatorV2.HasMorePages() {
			logInfo("⏳ Calling paginator.NextPage for unversioned objects...")
			ctxPage, cancel := context.WithTimeout(ctx, 120*time.Second)
			page, err := paginatorV2.NextPage(ctxPage)
			logInfo("✅ paginator.NextPage completed")
			cancel()
			if err != nil {
				logError("❌ Failed to retrieve page from ListObjectsV2: %v", err)
				return
			}

			logInfo("S3 V2 page read completed: %d objects", len(page.Contents))
			if len(page.Contents) == 0 {
				logInfo("No objects returned in this page — the bucket may be empty or fully processed.")
			}

			for _, obj := range page.Contents {
				key := aws.ToString(obj.Key)
				logger.Debug("Checking object: %s", key)
				logInfo("Queueing object: %s", key)
				currentBatch = append(currentBatch, types.ObjectIdentifier{
					Key: obj.Key,
				})
				totalCount++
				if len(currentBatch) >= batchSize {
					batchChan <- currentBatch
					logInfo("🔄 Batch sent with %d objects", len(currentBatch))
					currentBatch = nil
				}
			}

			logInfo("📄 V2 Page processed. Objects: %d", len(page.Contents))
		}

		logInfo("✅ Completed listing. Total objects queued: %d", totalCount)

		if len(currentBatch) > 0 {
			batchChan <- currentBatch
			logInfo("🔄 Final batch sent with %d objects", len(currentBatch))
		}
	}()
}
