package lister

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
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

// ScanStrategy selects how ParallelScan partitions the keyspace.
type ScanStrategy string

const (
	StrategyAuto      ScanStrategy = "auto"      // delimiter when bucket has folders, otherwise sharded
	StrategySerial    ScanStrategy = "serial"    // single-threaded (legacy Scan)
	StrategyDelimiter ScanStrategy = "delimiter" // partition by discovered top-level prefixes
	StrategySharded   ScanStrategy = "sharded"   // partition by first-byte prefix (256 shards)
)

// ScanProgress is reported by ParallelScan as shards complete. ShardsTotal is
// 0 until the strategy is decided.
type ScanProgress struct {
	KeysScanned int64
	ShardsDone  int
	ShardsTotal int
}

// ScanOptions configures ParallelScan. A zero value runs serial.
type ScanOptions struct {
	Concurrency int          // worker pool size; ≤ 1 forces serial
	Strategy    ScanStrategy // empty → StrategyAuto
	OnProgress  func(ScanProgress)
}

const autoDelimiterThreshold = 4 // sub-prefixes needed for delimiter strategy in auto mode

// Scan walks the bucket once (twice if versioned) and tallies totals serially.
// Preserved for backward compatibility and as the inner worker for parallel
// shards. When prefix is non-empty, only matching keys are counted.
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
		v, dm, err := countVersionsAt(ctx, client, bucket, prefix)
		if err != nil {
			return nil, err
		}
		inv.VersionedObjects = v
		inv.DeleteMarkers = dm
	}

	inv.Elapsed = time.Since(start)
	return inv, nil
}

// ParallelScan partitions the keyspace and scans shards concurrently. Dispatch
// is controlled by opts.Strategy:
//
//   - serial:    fall back to Scan
//   - delimiter: discover top-level prefixes via ListObjectsV2(Delimiter="/"),
//                then list each in parallel
//   - sharded:   list 256 single-byte prefix shards in parallel
//   - auto:      do a cheap delimiter discovery; if the bucket has ≥4
//                top-level prefixes use delimiter, otherwise fall back to
//                sharded
//
// Strategy is auto-resolved when empty.
func ParallelScan(ctx context.Context, client S3API, bucket, prefix string, versioned bool, opts ScanOptions) (*Inventory, error) {
	if opts.Concurrency <= 1 || opts.Strategy == StrategySerial {
		return Scan(ctx, client, bucket, prefix, versioned)
	}
	if opts.Strategy == "" {
		opts.Strategy = StrategyAuto
	}

	switch opts.Strategy {
	case StrategyDelimiter:
		subs, rootKeys, rootSize, err := discoverSubPrefixes(ctx, client, bucket, prefix)
		if err != nil {
			return nil, err
		}
		if len(subs) == 0 {
			return Scan(ctx, client, bucket, prefix, versioned)
		}
		return scanByDiscoveredFolders(ctx, client, bucket, prefix, versioned, opts, subs, rootKeys, rootSize)
	case StrategySharded:
		return scanByByteShards(ctx, client, bucket, prefix, versioned, opts)
	case StrategyAuto:
		subs, rootKeys, rootSize, err := discoverSubPrefixes(ctx, client, bucket, prefix)
		if err != nil {
			return nil, err
		}
		if len(subs) >= autoDelimiterThreshold {
			return scanByDiscoveredFolders(ctx, client, bucket, prefix, versioned, opts, subs, rootKeys, rootSize)
		}
		// Too few top-level folders to benefit from delimiter parallelism;
		// shard by first byte instead. The discovery call is wasted here but
		// the cost is negligible (≤ a few round-trips).
		return scanByByteShards(ctx, client, bucket, prefix, versioned, opts)
	}
	return nil, fmt.Errorf("unknown scan strategy: %s", opts.Strategy)
}

// discoverSubPrefixes paginates ListObjectsV2(Delimiter="/") and returns both
// the sub-prefixes and a count + size of keys directly under `prefix` (those
// without a further '/').
func discoverSubPrefixes(ctx context.Context, client S3API, bucket, prefix string) (subs []string, rootKeys, rootSize int64, err error) {
	var continuationToken *string
	for {
		ctxPage, cancel := context.WithTimeout(ctx, 120*time.Second)
		out, listErr := client.ListObjectsV2(ctxPage, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            stringOrNil(prefix),
			Delimiter:         aws.String("/"),
			ContinuationToken: continuationToken,
		})
		cancel()
		if listErr != nil {
			return nil, 0, 0, listErr
		}
		for _, cp := range out.CommonPrefixes {
			subs = append(subs, aws.ToString(cp.Prefix))
		}
		for _, obj := range out.Contents {
			rootKeys++
			rootSize += aws.ToInt64(obj.Size)
		}
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		continuationToken = out.NextContinuationToken
	}
	return subs, rootKeys, rootSize, nil
}

// scanByDiscoveredFolders runs a parallel scan across already-discovered sub
// prefixes (delimiter strategy). The sub-prefixes are by construction the
// top-level folders relative to `prefix`, so TopLevelFolders = len(subs).
func scanByDiscoveredFolders(ctx context.Context, client S3API, bucket, prefix string, versioned bool, opts ScanOptions, subs []string, rootKeys, rootSize int64) (*Inventory, error) {
	start := time.Now()
	inv := &Inventory{
		TotalObjects:    rootKeys,
		TotalSizeBytes:  rootSize,
		TopLevelFolders: len(subs),
	}
	var scanned int64 = rootKeys
	emitProgress(opts.OnProgress, scanned, 0, len(subs))

	if err := parallelScanShards(ctx, client, bucket, subs, opts, inv, &scanned, false, prefix); err != nil {
		return nil, err
	}

	if versioned {
		// Versions for keys directly under basePrefix that have no further
		// delimiter (these wouldn't appear in any sub-prefix's scan).
		rootV, rootDM, err := countRootVersions(ctx, client, bucket, prefix)
		if err != nil {
			return nil, err
		}
		var mu sync.Mutex
		mu.Lock()
		inv.VersionedObjects = rootV
		inv.DeleteMarkers = rootDM
		mu.Unlock()
		if err := parallelListVersionsShards(ctx, client, bucket, subs, opts, inv); err != nil {
			return nil, err
		}
	}

	inv.Elapsed = time.Since(start)
	return inv, nil
}

// scanByByteShards runs a parallel scan using 256 single-byte prefix shards.
// Top-level folders are computed by extracting the first segment from every
// observed key because shards do not align with the "/" delimiter.
func scanByByteShards(ctx context.Context, client S3API, bucket, prefix string, versioned bool, opts ScanOptions) (*Inventory, error) {
	start := time.Now()
	inv := &Inventory{}
	shards := byteShardPrefixes(prefix)
	var scanned int64
	emitProgress(opts.OnProgress, 0, 0, len(shards))

	if err := parallelScanShards(ctx, client, bucket, shards, opts, inv, &scanned, true, prefix); err != nil {
		return nil, err
	}

	if versioned {
		if err := parallelListVersionsShards(ctx, client, bucket, shards, opts, inv); err != nil {
			return nil, err
		}
	}

	inv.Elapsed = time.Since(start)
	return inv, nil
}

// byteShardPrefixes returns 256 prefixes of the form basePrefix + <byte>.
func byteShardPrefixes(basePrefix string) []string {
	shards := make([]string, 256)
	for i := 0; i < 256; i++ {
		shards[i] = basePrefix + string([]byte{byte(i)})
	}
	return shards
}

// parallelScanShards drives a concurrency-bounded pool that runs scanFlatPrefix
// for each shard. When collectFolders is true, results from every shard are
// unioned into inv.TopLevelFolders; otherwise the caller has already set it.
func parallelScanShards(ctx context.Context, client S3API, bucket string, shards []string, opts ScanOptions, inv *Inventory, scanned *int64, collectFolders bool, basePrefix string) error {
	var mu sync.Mutex
	var folders map[string]struct{}
	if collectFolders {
		folders = make(map[string]struct{})
	}
	sem := make(chan struct{}, opts.Concurrency)
	var wg sync.WaitGroup
	errCh := make(chan error, len(shards))
	var done int64

	for _, sh := range shards {
		sh := sh
		select {
		case <-ctx.Done():
			break
		case sem <- struct{}{}:
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			objects, size, segs, err := scanFlatPrefix(ctx, client, bucket, sh, basePrefix, collectFolders)
			if err != nil {
				errCh <- err
				return
			}
			mu.Lock()
			inv.TotalObjects += objects
			inv.TotalSizeBytes += size
			if collectFolders {
				for s := range segs {
					folders[s] = struct{}{}
				}
			}
			mu.Unlock()
			atomic.AddInt64(scanned, objects)
			d := atomic.AddInt64(&done, 1)
			emitProgress(opts.OnProgress, atomic.LoadInt64(scanned), int(d), len(shards))
		}()
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		if e != nil {
			return e
		}
	}
	if collectFolders {
		mu.Lock()
		inv.TopLevelFolders = len(folders)
		mu.Unlock()
	}
	return nil
}

// scanFlatPrefix paginates ListObjectsV2 with a single Prefix (no Delimiter).
// segments is populated only when collectFolders is true.
func scanFlatPrefix(ctx context.Context, client S3API, bucket, listPrefix, basePrefix string, collectFolders bool) (objects, size int64, segments map[string]struct{}, err error) {
	if collectFolders {
		segments = make(map[string]struct{})
	}
	var continuationToken *string
	for {
		ctxPage, cancel := context.WithTimeout(ctx, 120*time.Second)
		out, listErr := client.ListObjectsV2(ctxPage, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            stringOrNil(listPrefix),
			ContinuationToken: continuationToken,
		})
		cancel()
		if listErr != nil {
			return 0, 0, nil, listErr
		}
		for _, obj := range out.Contents {
			objects++
			size += aws.ToInt64(obj.Size)
			if collectFolders {
				if seg := topLevelSegment(aws.ToString(obj.Key), basePrefix); seg != "" {
					segments[seg] = struct{}{}
				}
			}
		}
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		continuationToken = out.NextContinuationToken
	}
	return objects, size, segments, nil
}

// parallelListVersionsShards runs ListObjectVersions in parallel across the
// supplied shard prefixes, summing version + delete-marker counts into inv.
func parallelListVersionsShards(ctx context.Context, client S3API, bucket string, shards []string, opts ScanOptions, inv *Inventory) error {
	var mu sync.Mutex
	sem := make(chan struct{}, opts.Concurrency)
	var wg sync.WaitGroup
	errCh := make(chan error, len(shards))

	for _, sh := range shards {
		sh := sh
		select {
		case <-ctx.Done():
			break
		case sem <- struct{}{}:
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			versions, markers, err := countVersionsAt(ctx, client, bucket, sh)
			if err != nil {
				errCh <- err
				return
			}
			mu.Lock()
			inv.VersionedObjects += versions
			inv.DeleteMarkers += markers
			mu.Unlock()
		}()
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		if e != nil {
			return e
		}
	}
	return nil
}

// countVersionsAt paginates ListObjectVersions for a single prefix and returns
// total versions and delete-marker counts.
func countVersionsAt(ctx context.Context, client S3API, bucket, prefix string) (versions, markers int64, err error) {
	var keyMarker, versionIdMarker *string
	for {
		ctxPage, cancel := context.WithTimeout(ctx, 120*time.Second)
		out, listErr := client.ListObjectVersions(ctxPage, &s3.ListObjectVersionsInput{
			Bucket:          aws.String(bucket),
			Prefix:          stringOrNil(prefix),
			KeyMarker:       keyMarker,
			VersionIdMarker: versionIdMarker,
		})
		cancel()
		if listErr != nil {
			return 0, 0, listErr
		}
		versions += int64(len(out.Versions))
		markers += int64(len(out.DeleteMarkers))
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		keyMarker = out.NextKeyMarker
		versionIdMarker = out.NextVersionIdMarker
	}
	return versions, markers, nil
}

// countRootVersions returns versions/markers for keys that live directly under
// basePrefix (no further '/'). Used by the delimiter strategy to capture
// entries that aren't covered by any sub-prefix scan.
func countRootVersions(ctx context.Context, client S3API, bucket, basePrefix string) (versions, markers int64, err error) {
	var keyMarker, versionIdMarker *string
	for {
		ctxPage, cancel := context.WithTimeout(ctx, 120*time.Second)
		out, listErr := client.ListObjectVersions(ctxPage, &s3.ListObjectVersionsInput{
			Bucket:          aws.String(bucket),
			Prefix:          stringOrNil(basePrefix),
			Delimiter:       aws.String("/"),
			KeyMarker:       keyMarker,
			VersionIdMarker: versionIdMarker,
		})
		cancel()
		if listErr != nil {
			return 0, 0, listErr
		}
		versions += int64(len(out.Versions))
		markers += int64(len(out.DeleteMarkers))
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		keyMarker = out.NextKeyMarker
		versionIdMarker = out.NextVersionIdMarker
	}
	return versions, markers, nil
}

func emitProgress(fn func(ScanProgress), scanned int64, done, total int) {
	if fn == nil {
		return
	}
	fn(ScanProgress{KeysScanned: scanned, ShardsDone: done, ShardsTotal: total})
}

// topLevelSegment returns the first path component after prefix.
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
