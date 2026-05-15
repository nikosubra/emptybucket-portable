// Package runner orchestrates a single end-to-end bucket-empty operation
// (client init + inventory + deletion engine), emitting structured events on
// a channel so multiple front-ends (CLI, TUI, Web UI) can share one core.
package runner

import (
	"context"
	"crypto/tls"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/logging"

	"github.com/nikosubra/emptybucket-portable/awscli"
	"github.com/nikosubra/emptybucket-portable/deleter"
	"github.com/nikosubra/emptybucket-portable/lister"
	"github.com/nikosubra/emptybucket-portable/logger"
)

type Request struct {
	AccessKey    string `json:"accessKey"`
	SecretKey    string `json:"secretKey"`
	SessionToken string `json:"sessionToken,omitempty"` // optional STS session token
	Bucket       string `json:"bucket"`
	Endpoint     string `json:"endpoint"`
	Region       string `json:"region"`
	Engine       string `json:"engine"` // "sdk" | "awscli" | "auto"
	Workers      int    `json:"workers"`
	BatchSize    int    `json:"batchSize"`
	DryRun       bool   `json:"dryRun"`
	Insecure         bool   `json:"insecure"`         // skip TLS verification
	Retries          int    `json:"retries"`          // delete-batch retry attempts; 0 = default (3)
	Prefix           string `json:"prefix"`           // optional key prefix filter (e.g. "logs/")
	ScanConcurrency  int    `json:"scanConcurrency"`  // parallel scan workers; 0 = default (8)
	ScanStrategy     string `json:"scanStrategy"`     // "auto" | "serial" | "delimiter" | "sharded"
	SkipInventory    bool   `json:"skipInventory"`    // start deletion immediately, no totals/ETA
}

// Validate normalizes and checks the request. Returns an error message
// suitable for showing to end-users; returns nil when the request is usable.
func (r *Request) Validate() error {
	r.Bucket = strings.TrimSpace(r.Bucket)
	r.Bucket = strings.TrimPrefix(r.Bucket, "s3://")
	r.Bucket = strings.TrimSuffix(r.Bucket, "/")
	r.Endpoint = strings.TrimSpace(r.Endpoint)
	r.Region = strings.TrimSpace(r.Region)
	r.AccessKey = strings.TrimSpace(r.AccessKey)
	r.SecretKey = strings.TrimSpace(r.SecretKey)
	r.Prefix = strings.TrimSpace(r.Prefix)
	r.Prefix = strings.TrimPrefix(r.Prefix, "/")

	if r.Endpoint != "" && !strings.Contains(r.Endpoint, "://") {
		r.Endpoint = "https://" + r.Endpoint
	}

	switch {
	case r.Bucket == "":
		return fmt.Errorf("bucket is required")
	case r.Endpoint == "":
		return fmt.Errorf("endpoint is required")
	case r.AccessKey == "":
		return fmt.Errorf("access key is required")
	case r.SecretKey == "":
		return fmt.Errorf("secret key is required")
	}
	if r.Region == "" {
		r.Region = "us-east-1"
	}
	if r.Workers <= 0 {
		r.Workers = 4
	}
	if r.BatchSize <= 0 {
		r.BatchSize = 200
	}
	if r.BatchSize > 1000 {
		r.BatchSize = 1000
	}
	if r.Engine == "" {
		r.Engine = "sdk"
	}
	switch r.Engine {
	case "sdk", "awscli", "auto":
	default:
		return fmt.Errorf("engine must be one of: sdk, awscli, auto")
	}
	if r.ScanConcurrency <= 0 {
		r.ScanConcurrency = 8
	}
	if r.ScanStrategy == "" {
		r.ScanStrategy = "auto"
	}
	switch r.ScanStrategy {
	case "auto", "serial", "delimiter", "sharded":
	default:
		return fmt.Errorf("scan-strategy must be one of: auto, serial, delimiter, sharded")
	}
	return nil
}

// EventKind enumerates lifecycle events emitted on the runner channel.
type EventKind string

const (
	EventStarted      EventKind = "started"
	EventScanProgress EventKind = "scanProgress"
	EventInventory    EventKind = "inventory"
	EventDeletion     EventKind = "deletion"
	EventStats        EventKind = "stats"
	EventFinished     EventKind = "finished"
	EventError        EventKind = "error"
)

// Event is the union payload emitted on the channel. Only fields relevant to
// Kind are populated; consumers should switch on Kind.
type Event struct {
	Kind      EventKind              `json:"kind"`
	Message   string                 `json:"message,omitempty"`
	Inventory *lister.Inventory      `json:"inventory,omitempty"`
	Deletion  *deleter.DeletionEvent `json:"deletion,omitempty"`
	Stats     *Stats                 `json:"stats,omitempty"`
	Scan      *lister.ScanProgress   `json:"scan,omitempty"`
}

// Stats is a periodic snapshot used by UIs to update progress widgets.
type Stats struct {
	Deleted       int64         `json:"deleted"`
	Errors        int64         `json:"errors"`
	Total         int64         `json:"total"`
	ObjectsPerSec float64       `json:"objectsPerSec"`
	Elapsed       time.Duration `json:"elapsedNs"`
	ETA           time.Duration `json:"etaNs"`
}

// FailedKey is a single failed deletion attempt, captured for later CSV export.
type FailedKey struct {
	Key       string
	VersionId string
	Reason    string
}

// Result is the final outcome returned after the channel closes.
type Result struct {
	Deleted    int
	Errors     int
	Duration   time.Duration
	Inventory  *lister.Inventory
	Versioned  bool
	Engine     string // engine actually used after auto-resolution
	FailedKeys []FailedKey
}

func initS3Client(req Request) (*s3.Client, error) {
	httpClient := &http.Client{}
	if req.Insecure {
		httpClient.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
	}
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(req.Region),
		config.WithCredentialsProvider(
			aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(req.AccessKey, req.SecretKey, req.SessionToken)),
		),
		config.WithEndpointResolver(aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
			return aws.Endpoint{URL: req.Endpoint, HostnameImmutable: true}, nil
		})),
		config.WithHTTPClient(httpClient),
		config.WithLogger(logging.NewStandardLogger(os.Stderr)),
	)
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(cfg), nil
}

// Run executes the full pipeline. Events flow on `events` (closed when done).
// The function is synchronous: callers can spawn it in a goroutine to overlap
// with a UI event loop. A final EventFinished is always emitted before the
// channel is closed, even on early failure, so UIs can reliably reset state.
func Run(ctx context.Context, req Request, events chan<- Event) Result {
	start := time.Now()

	send := func(e Event) {
		select {
		case events <- e:
		case <-ctx.Done():
		}
	}

	var result Result
	var total int64
	defer func() {
		result.Duration = time.Since(start)
		send(Event{Kind: EventFinished, Stats: &Stats{
			Deleted: int64(result.Deleted), Errors: int64(result.Errors),
			Total: total, Elapsed: result.Duration,
		}})
		close(events)
	}()

	if err := req.Validate(); err != nil {
		send(Event{Kind: EventError, Message: "invalid request: " + err.Error()})
		return result
	}

	send(Event{Kind: EventStarted, Message: fmt.Sprintf("Connecting to %s/%s", req.Endpoint, req.Bucket)})

	client, err := initS3Client(req)
	if err != nil {
		send(Event{Kind: EventError, Message: "s3 client init: " + err.Error()})
		return result
	}
	if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(req.Bucket)}); err != nil {
		send(Event{Kind: EventError, Message: classifyBucketError(req.Bucket, err)})
		return result
	}

	verCfg, _ := client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(req.Bucket)})
	versioned := verCfg != nil && verCfg.Status == "Enabled"

	// Inventory (optional).
	var inv *lister.Inventory
	if req.SkipInventory {
		send(Event{Kind: EventStarted, Message: "Skipping inventory scan (--skip-inventory); ETA and totals will be unavailable."})
	} else {
		send(Event{Kind: EventStarted, Message: fmt.Sprintf("Scanning bucket inventory (strategy=%s, concurrency=%d)...", req.ScanStrategy, req.ScanConcurrency)})
		scanned, scanErr := lister.ParallelScan(ctx, client, req.Bucket, req.Prefix, versioned, lister.ScanOptions{
			Concurrency: req.ScanConcurrency,
			Strategy:    lister.ScanStrategy(req.ScanStrategy),
			OnProgress: func(p lister.ScanProgress) {
				snap := p
				send(Event{Kind: EventScanProgress, Scan: &snap})
			},
		})
		if scanErr != nil {
			send(Event{Kind: EventError, Message: "inventory: " + scanErr.Error()})
			// Continue without inventory — deletion still possible.
		} else {
			inv = scanned
			send(Event{Kind: EventInventory, Inventory: inv})
		}
	}

	if inv != nil {
		if versioned {
			total = inv.VersionedObjects + inv.DeleteMarkers
		} else {
			total = inv.TotalObjects
		}
	}

	// Resolve engine.
	engine := req.Engine
	if engine == "auto" {
		if _, err := awscli.Detect(); err == nil {
			engine = "awscli"
		} else {
			engine = "sdk"
		}
	}

	// Bridge per-object deletion events to our Event channel for the UI feed,
	// and accumulate failed keys for later CSV export. The bridge does NOT
	// count successful deletions: under load EventChan emits are non-blocking
	// and may drop. Live counters are written directly by the engine via
	// LiveDeleted / LiveErrors below.
	delEvents := make(chan deleter.DeletionEvent, 256)
	var deletedAtomic, errorAtomic int64
	var failedMu sync.Mutex
	var failedKeys []FailedKey
	var bridgeWG sync.WaitGroup
	bridgeWG.Add(1)
	go func() {
		defer bridgeWG.Done()
		for ev := range delEvents {
			if ev.Failed {
				failedMu.Lock()
				failedKeys = append(failedKeys, FailedKey{Key: ev.Key, VersionId: ev.VersionId, Reason: ev.Err})
				failedMu.Unlock()
			}
			ev := ev
			send(Event{Kind: EventDeletion, Deletion: &ev})
		}
	}()

	var deletedCount, errorCount int
	statsDone := make(chan struct{})
	bridgeWG.Add(1)
	go func() {
		defer bridgeWG.Done()
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-statsDone:
				return
			case <-ticker.C:
				d := atomic.LoadInt64(&deletedAtomic)
				e := atomic.LoadInt64(&errorAtomic)
				elapsed := time.Since(start)
				var rate float64
				var eta time.Duration
				if elapsed.Seconds() > 0 && d > 0 {
					rate = float64(d) / elapsed.Seconds()
					if total > 0 && rate > 0 {
						eta = time.Duration(float64(total-d)/rate) * time.Second
					}
				}
				send(Event{Kind: EventStats, Stats: &Stats{
					Deleted: d, Errors: e, Total: total,
					ObjectsPerSec: rate, Elapsed: elapsed, ETA: eta,
				}})
			}
		}
	}()

	switch engine {
	case "awscli":
		res, err := awscli.Run(ctx, awscli.Config{
			AccessKey: req.AccessKey, SecretKey: req.SecretKey, SessionToken: req.SessionToken,
			Region: req.Region, Endpoint: req.Endpoint, Bucket: req.Bucket, Prefix: req.Prefix,
			Versioned: versioned, InsecureTLS: req.Insecure, Workers: req.Workers, BatchSize: req.BatchSize,
			DryRun: req.DryRun, TotalObjects: total, StartTime: start,
			EventChan: delEvents, S3Client: client,
			Logger: logger.Info, Warn: logger.Warn, Errorf: logger.Error,
			LiveDeleted: &deletedAtomic, LiveErrors: &errorAtomic,
		})
		close(delEvents)
		if err != nil {
			send(Event{Kind: EventError, Message: "awscli engine: " + err.Error()})
		}
		deletedCount, errorCount = res.DeletedCount, res.ErrorCount

	default: // sdk
		batchChan := make(chan []types.ObjectIdentifier, 10)
		lister.StartProducer(ctx, client, req.Bucket, req.Prefix, req.BatchSize, batchChan, logger.Info, logger.Error, versioned)
		res := deleter.Run(ctx, deleter.Config{
			Client: client, Bucket: req.Bucket, BatchSize: req.BatchSize,
			NumWorkers: req.Workers, DryRun: req.DryRun, Retries: req.Retries,
			TotalObjects: total, StartTime: start,
			EventChan:   delEvents,
			LiveDeleted: &deletedAtomic, LiveErrors: &errorAtomic,
		}, batchChan, logger.Info, logger.Warn, logger.Error)
		deletedCount, errorCount = res.DeletedCount, res.ErrorCount
	}

	close(statsDone)
	bridgeWG.Wait()

	result.Deleted = deletedCount
	result.Errors = errorCount
	result.Inventory = inv
	result.Versioned = versioned
	result.Engine = engine
	failedMu.Lock()
	result.FailedKeys = failedKeys
	failedMu.Unlock()
	return result
}


// WriteArtifacts persists run outputs (failures.csv and metrics.json) to
// outDir. Pass an empty string to skip. Returns nil on success or when there
// is nothing to write.
func WriteArtifacts(outDir string, req Request, res Result) error {
	if outDir == "" {
		return nil
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", outDir, err)
	}

	// failures.csv
	if len(res.FailedKeys) > 0 {
		f, err := os.Create(filepath.Join(outDir, "failures.csv"))
		if err != nil {
			return err
		}
		w := csv.NewWriter(f)
		_ = w.Write([]string{"Key", "VersionId", "Reason"})
		for _, fk := range res.FailedKeys {
			_ = w.Write([]string{fk.Key, fk.VersionId, fk.Reason})
		}
		w.Flush()
		f.Close()
	}

	// metrics.json
	mf, err := os.Create(filepath.Join(outDir, "metrics.json"))
	if err != nil {
		return err
	}
	defer mf.Close()
	m := map[string]interface{}{
		"timestamp":      time.Now().Format(time.RFC3339),
		"duration":       res.Duration.Truncate(time.Second).String(),
		"deleted":        res.Deleted,
		"errors":         res.Errors,
		"failuresLogged": len(res.FailedKeys),
		"engine":         res.Engine,
		"versioned":      res.Versioned,
		"dryRun":         req.DryRun,
		"bucket":         req.Bucket,
		"endpoint":       req.Endpoint,
		"region":         req.Region,
	}
	if res.Inventory != nil {
		m["totalObjects"] = res.Inventory.TotalObjects
		m["topLevelFolders"] = res.Inventory.TopLevelFolders
		m["totalSizeBytes"] = res.Inventory.TotalSizeBytes
		m["versionedObjects"] = res.Inventory.VersionedObjects
		m["deleteMarkers"] = res.Inventory.DeleteMarkers
	}
	enc := json.NewEncoder(mf)
	enc.SetIndent("", "  ")
	return enc.Encode(m)
}

// HumanBytes renders a byte count in IEC binary units (KiB, MiB, ...).
func HumanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// classifyBucketError turns an opaque HeadBucket error into a sysadmin-friendly
// message. The S3 SDK surfaces HTTP 404/403/301/400 with no typed errors, so we
// match on the wrapped status text and known error codes.
func classifyBucketError(bucket string, err error) string {
	msg := err.Error()
	low := strings.ToLower(msg)
	switch {
	case strings.Contains(low, "status code: 404"), strings.Contains(low, "nosuchbucket"), strings.Contains(low, "notfound"):
		return fmt.Sprintf("bucket %q not found — check the name and endpoint", bucket)
	case strings.Contains(low, "status code: 403"), strings.Contains(low, "accessdenied"), strings.Contains(low, "forbidden"):
		return fmt.Sprintf("access denied on bucket %q — verify credentials and IAM/policy permissions (s3:ListBucket required)", bucket)
	case strings.Contains(low, "status code: 301"), strings.Contains(low, "permanentredirect"):
		return fmt.Sprintf("bucket %q is in a different region — set --region to match", bucket)
	case strings.Contains(low, "status code: 400"):
		return fmt.Sprintf("bad request on bucket %q — check endpoint URL and bucket-name format: %v", bucket, err)
	case strings.Contains(low, "no such host"), strings.Contains(low, "dial tcp"), strings.Contains(low, "connection refused"):
		return fmt.Sprintf("cannot reach endpoint — check --endpoint and network: %v", err)
	case strings.Contains(low, "x509"), strings.Contains(low, "certificate"):
		return fmt.Sprintf("TLS certificate error — use --insecure for self-signed endpoints: %v", err)
	default:
		return fmt.Sprintf("bucket %q not accessible: %v", bucket, err)
	}
}
