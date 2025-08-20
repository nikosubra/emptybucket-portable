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
		logInfo("🔍 StartProducer invoked for bucket: %s — using manual pagination with ListObjects (v1)", bucket)
		defer close(batchChan)

		var totalCount int
		var currentBatch []types.ObjectIdentifier
		var marker *string

		for {
			logInfo("⏳ Calling ListObjects with Marker: %v", marker)
			ctxPage, cancel := context.WithTimeout(ctx, 120*time.Second)
			output, err := client.ListObjects(ctxPage, &s3.ListObjectsInput{
				Bucket: aws.String(bucket),
				Marker: marker,
			})
			cancel()
			if err != nil {
				logError("❌ Failed to retrieve page from ListObjects: %v", err)
				return
			}

			logInfo("S3 V1 page read completed: %d objects", len(output.Contents))
			if len(output.Contents) == 0 {
				logInfo("No objects returned in this page — the bucket may be empty or fully processed.")
			}

			for _, obj := range output.Contents {
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

			if aws.ToBool(output.IsTruncated) == false {
				break
			}
			marker = output.Contents[len(output.Contents)-1].Key
			logInfo("📄 Page processed. Objects: %d, Next Marker: %s", len(output.Contents), aws.ToString(marker))
		}

		logInfo("✅ Completed listing. Total objects queued: %d", totalCount)

		if len(currentBatch) > 0 {
			batchChan <- currentBatch
			logInfo("🔄 Final batch sent with %d objects", len(currentBatch))
		}
	}()
}
