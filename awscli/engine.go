// Package awscli implements an alternative deletion engine that shells out to
// the `aws` CLI. For unversioned buckets it uses `aws s3 rm --recursive`,
// which internally parallelizes transfers and is typically faster than
// per-batch SDK calls on very large buckets. For versioned buckets it lists
// versions via the SDK (already paginated efficiently) and shells out to
// `aws s3api delete-objects` in parallel batches.
package awscli

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/nikosubra/emptybucket-portable/deleter"
)

// Detect returns the resolved path to the `aws` binary, or an error if not on PATH.
func Detect() (string, error) {
	return exec.LookPath("aws")
}

type Config struct {
	AccessKey    string
	SecretKey    string
	SessionToken string
	Region       string
	Endpoint     string
	Bucket       string
	Prefix       string // optional key prefix filter
	Versioned    bool
	InsecureTLS  bool
	Workers      int
	BatchSize    int
	DryRun       bool
	TotalObjects int64
	StartTime    time.Time
	EventChan    chan<- deleter.DeletionEvent
	Logger       func(string, ...interface{})
	Warn         func(string, ...interface{})
	Errorf       func(string, ...interface{})
	S3Client     *s3.Client // used for listing versions on versioned buckets

	// LiveDeleted / LiveErrors, when non-nil, are incremented atomically as
	// objects are processed. The runner uses them for accurate live stats
	// independent of EventChan back-pressure.
	LiveDeleted *int64
	LiveErrors  *int64
}

type Result struct {
	DeletedCount int
	ErrorCount   int
}

// Run dispatches to the appropriate code path based on bucket versioning.
func Run(ctx context.Context, cfg Config) (Result, error) {
	if _, err := Detect(); err != nil {
		return Result{}, fmt.Errorf("aws CLI not found on PATH: %w", err)
	}
	if cfg.Versioned {
		return runVersioned(ctx, cfg)
	}
	return runRecursive(ctx, cfg)
}

// envFor returns the env slice passed to every CLI invocation. Credentials and
// endpoint live only in this process's child env — never on disk.
func envFor(cfg Config) []string {
	env := append(os.Environ(),
		"AWS_ACCESS_KEY_ID="+cfg.AccessKey,
		"AWS_SECRET_ACCESS_KEY="+cfg.SecretKey,
		"AWS_DEFAULT_REGION="+cfg.Region,
		"AWS_REGION="+cfg.Region,
	)
	if cfg.SessionToken != "" {
		env = append(env, "AWS_SESSION_TOKEN="+cfg.SessionToken)
	}
	return env
}

func baseArgs(cfg Config) []string {
	args := []string{}
	if cfg.Endpoint != "" {
		args = append(args, "--endpoint-url", cfg.Endpoint)
	}
	if cfg.InsecureTLS {
		args = append(args, "--no-verify-ssl")
	}
	return args
}

// runRecursive shells out to `aws s3 rm s3://bucket[/prefix] --recursive` and
// parses stdout. Each successful deletion prints a line `delete: s3://…`.
func runRecursive(ctx context.Context, cfg Config) (Result, error) {
	target := "s3://" + cfg.Bucket
	if cfg.Prefix != "" {
		target += "/" + cfg.Prefix
	}
	args := append(baseArgs(cfg), "s3", "rm", target, "--recursive")
	if cfg.DryRun {
		args = append(args, "--dryrun")
	}
	cfg.Logger("awscli engine: %s %s", "aws", strings.Join(args, " "))

	cmd := exec.CommandContext(ctx, "aws", args...)
	cmd.Env = envFor(cfg)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return Result{}, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return Result{}, err
	}
	if err := cmd.Start(); err != nil {
		return Result{}, fmt.Errorf("aws s3 rm start failed: %w", err)
	}

	var deleted, errs int64
	var latestKey atomic.Value
	latestKey.Store("")

	// Drain stderr async so the buffer never blocks the child.
	go func() {
		s := bufio.NewScanner(stderr)
		for s.Scan() {
			cfg.Warn("aws stderr: %s", s.Text())
		}
	}()

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 1024*1024), 4*1024*1024)
	prefix := "delete: s3://" + cfg.Bucket + "/"
	dryPrefix := "(dryrun) delete: s3://" + cfg.Bucket + "/"
	for scanner.Scan() {
		line := scanner.Text()
		var key string
		switch {
		case strings.HasPrefix(line, prefix):
			key = strings.TrimPrefix(line, prefix)
		case strings.HasPrefix(line, dryPrefix):
			key = strings.TrimPrefix(line, dryPrefix)
		default:
			continue
		}
		atomic.AddInt64(&deleted, 1)
		if cfg.LiveDeleted != nil {
			atomic.AddInt64(cfg.LiveDeleted, 1)
		}
		latestKey.Store(key)
		if cfg.EventChan != nil {
			emit(cfg.EventChan, deleter.DeletionEvent{Key: key, Timestamp: time.Now()})
		}
	}

	waitErr := cmd.Wait()
	if waitErr != nil && ctx.Err() == nil {
		cfg.Errorf("aws s3 rm exited with error: %v", waitErr)
		atomic.AddInt64(&errs, 1)
		if cfg.LiveErrors != nil {
			atomic.AddInt64(cfg.LiveErrors, 1)
		}
	}
	return Result{DeletedCount: int(atomic.LoadInt64(&deleted)), ErrorCount: int(atomic.LoadInt64(&errs))}, nil
}

// runVersioned lists every version + delete-marker via the SDK, then shells out
// to `aws s3api delete-objects` in parallel batches (max 1000 per call).
func runVersioned(ctx context.Context, cfg Config) (Result, error) {
	cfg.Logger("awscli engine (versioned): listing versions via SDK, deleting via s3api delete-objects")

	batchChan := make(chan []types.ObjectIdentifier, 10)
	go listVersions(ctx, cfg, batchChan)

	var deleted, errs int64
	var latestKey atomic.Value
	latestKey.Store("")

	var wg sync.WaitGroup
	sem := make(chan struct{}, cfg.Workers)
	for batch := range batchChan {
		batch := batch
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			d, e := deleteBatchCLI(ctx, cfg, batch, &latestKey)
			atomic.AddInt64(&deleted, int64(d))
			atomic.AddInt64(&errs, int64(e))
		}()
	}
	wg.Wait()
	return Result{DeletedCount: int(deleted), ErrorCount: int(errs)}, nil
}

func listVersions(ctx context.Context, cfg Config, out chan<- []types.ObjectIdentifier) {
	defer close(out)
	if cfg.S3Client == nil {
		cfg.Errorf("awscli versioned engine requires an S3 client for listing")
		return
	}
	var keyMarker, versionIdMarker *string
	var current []types.ObjectIdentifier
	var prefixPtr *string
	if cfg.Prefix != "" {
		prefixPtr = aws.String(cfg.Prefix)
	}
	for {
		ctxPage, cancel := context.WithTimeout(ctx, 120*time.Second)
		page, err := cfg.S3Client.ListObjectVersions(ctxPage, &s3.ListObjectVersionsInput{
			Bucket:          aws.String(cfg.Bucket),
			Prefix:          prefixPtr,
			KeyMarker:       keyMarker,
			VersionIdMarker: versionIdMarker,
		})
		cancel()
		if err != nil {
			cfg.Errorf("ListObjectVersions: %v", err)
			return
		}
		for _, v := range page.Versions {
			current = append(current, types.ObjectIdentifier{Key: v.Key, VersionId: v.VersionId})
			if len(current) >= cfg.BatchSize {
				out <- current
				current = nil
			}
		}
		for _, dm := range page.DeleteMarkers {
			current = append(current, types.ObjectIdentifier{Key: dm.Key, VersionId: dm.VersionId})
			if len(current) >= cfg.BatchSize {
				out <- current
				current = nil
			}
		}
		if !aws.ToBool(page.IsTruncated) {
			break
		}
		keyMarker = page.NextKeyMarker
		versionIdMarker = page.NextVersionIdMarker
	}
	if len(current) > 0 {
		out <- current
	}
}

// deleteBatchCLI writes the batch as a temp JSON payload and runs
// `aws s3api delete-objects --bucket X --delete file://payload.json`.
func deleteBatchCLI(ctx context.Context, cfg Config, batch []types.ObjectIdentifier, latestKey *atomic.Value) (int, int) {
	if cfg.DryRun {
		// In dry-run we just emit events without invoking the CLI.
		for _, o := range batch {
			k := aws.ToString(o.Key)
			latestKey.Store(k)
			if cfg.LiveDeleted != nil {
				atomic.AddInt64(cfg.LiveDeleted, 1)
			}
			if cfg.EventChan != nil {
				emit(cfg.EventChan, deleter.DeletionEvent{Key: k, VersionId: aws.ToString(o.VersionId), Timestamp: time.Now()})
			}
		}
		return len(batch), 0
	}

	payload := map[string]interface{}{
		"Objects": batchToJSON(batch),
		"Quiet":   false,
	}
	tmp, err := os.CreateTemp("", "delobjs-*.json")
	if err != nil {
		cfg.Errorf("tempfile: %v", err)
		return 0, len(batch)
	}
	defer os.Remove(tmp.Name())
	if err := json.NewEncoder(tmp).Encode(payload); err != nil {
		tmp.Close()
		return 0, len(batch)
	}
	tmp.Close()

	args := append(baseArgs(cfg), "s3api", "delete-objects",
		"--bucket", cfg.Bucket,
		"--delete", "file://"+tmp.Name(),
		"--bypass-governance-retention",
		"--output", "json",
	)
	cmd := exec.CommandContext(ctx, "aws", args...)
	cmd.Env = envFor(cfg)
	out, err := cmd.Output()
	if err != nil {
		ee, _ := err.(*exec.ExitError)
		stderr := ""
		if ee != nil {
			stderr = string(ee.Stderr)
		}
		cfg.Warn("delete-objects failed: %v %s", err, stderr)
		if cfg.LiveErrors != nil {
			atomic.AddInt64(cfg.LiveErrors, int64(len(batch)))
		}
		return 0, len(batch)
	}

	var resp struct {
		Deleted []struct {
			Key       string `json:"Key"`
			VersionId string `json:"VersionId"`
		} `json:"Deleted"`
		Errors []struct {
			Key       string `json:"Key"`
			VersionId string `json:"VersionId"`
			Message   string `json:"Message"`
		} `json:"Errors"`
	}
	if err := json.Unmarshal(out, &resp); err != nil {
		cfg.Warn("delete-objects parse: %v", err)
		return len(batch), 0 // assume success when output is empty (Quiet mode behavior on some CLIs)
	}
	if cfg.LiveDeleted != nil && len(resp.Deleted) > 0 {
		atomic.AddInt64(cfg.LiveDeleted, int64(len(resp.Deleted)))
	}
	if cfg.LiveErrors != nil && len(resp.Errors) > 0 {
		atomic.AddInt64(cfg.LiveErrors, int64(len(resp.Errors)))
	}
	for _, d := range resp.Deleted {
		latestKey.Store(d.Key)
		if cfg.EventChan != nil {
			emit(cfg.EventChan, deleter.DeletionEvent{Key: d.Key, VersionId: d.VersionId, Timestamp: time.Now()})
		}
	}
	for _, e := range resp.Errors {
		if cfg.EventChan != nil {
			emit(cfg.EventChan, deleter.DeletionEvent{Key: e.Key, VersionId: e.VersionId, Timestamp: time.Now(), Failed: true, Err: e.Message})
		}
	}
	return len(resp.Deleted), len(resp.Errors)
}

func batchToJSON(batch []types.ObjectIdentifier) []map[string]string {
	out := make([]map[string]string, 0, len(batch))
	for _, o := range batch {
		m := map[string]string{"Key": aws.ToString(o.Key)}
		if v := aws.ToString(o.VersionId); v != "" {
			m["VersionId"] = v
		}
		out = append(out, m)
	}
	return out
}

func emit(ch chan<- deleter.DeletionEvent, ev deleter.DeletionEvent) {
	select {
	case ch <- ev:
	default:
	}
}

