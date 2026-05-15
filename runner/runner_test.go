package runner

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nikosubra/emptybucket-portable/lister"
)

func TestValidate_AppliesDefaults(t *testing.T) {
	r := Request{
		AccessKey: "AKIA",
		SecretKey: "secret",
		Bucket:    "  s3://my-bucket/  ",
		Endpoint:  "s3.example.com",
	}
	if err := r.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.Bucket != "my-bucket" {
		t.Errorf("bucket normalization: got %q want %q", r.Bucket, "my-bucket")
	}
	if r.Endpoint != "https://s3.example.com" {
		t.Errorf("endpoint scheme: got %q", r.Endpoint)
	}
	if r.Region != "us-east-1" {
		t.Errorf("region default: got %q", r.Region)
	}
	if r.Workers != 4 || r.BatchSize != 200 || r.Engine != "sdk" {
		t.Errorf("defaults not applied: workers=%d batch=%d engine=%q", r.Workers, r.BatchSize, r.Engine)
	}
}

func TestValidate_RequiredFields(t *testing.T) {
	cases := []struct {
		name string
		req  Request
		want string
	}{
		{"missing bucket", Request{AccessKey: "a", SecretKey: "b", Endpoint: "https://x"}, "bucket is required"},
		{"missing endpoint", Request{AccessKey: "a", SecretKey: "b", Bucket: "x"}, "endpoint is required"},
		{"missing access key", Request{SecretKey: "b", Bucket: "x", Endpoint: "https://x"}, "access key is required"},
		{"missing secret key", Request{AccessKey: "a", Bucket: "x", Endpoint: "https://x"}, "secret key is required"},
		{"bad engine", Request{AccessKey: "a", SecretKey: "b", Bucket: "x", Endpoint: "https://x", Engine: "nope"}, "engine must be one of"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.req.Validate()
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.want)
			}
		})
	}
}

func TestValidate_BatchSizeClamp(t *testing.T) {
	r := Request{AccessKey: "a", SecretKey: "b", Bucket: "x", Endpoint: "https://x", BatchSize: 5000}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	if r.BatchSize != 1000 {
		t.Errorf("batch size not clamped to 1000: got %d", r.BatchSize)
	}
}

func TestHumanBytes(t *testing.T) {
	cases := []struct {
		in   int64
		want string
	}{
		{0, "0 B"},
		{500, "500 B"},
		{1024, "1.00 KiB"},
		{1024 * 1024, "1.00 MiB"},
		{1536, "1.50 KiB"},
	}
	for _, tc := range cases {
		got := HumanBytes(tc.in)
		if got != tc.want {
			t.Errorf("HumanBytes(%d) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestWriteArtifacts_FailuresAndMetrics(t *testing.T) {
	dir := t.TempDir()
	req := Request{
		AccessKey: "a", SecretKey: "b", Bucket: "my-bucket",
		Endpoint: "https://s3.example.com", Region: "eu-west-1",
		Engine: "sdk", DryRun: true,
	}
	res := Result{
		Deleted: 42, Errors: 2, Duration: 3 * time.Second,
		Engine: "sdk", Versioned: false,
		Inventory: &lister.Inventory{TotalObjects: 100, TopLevelFolders: 3, TotalSizeBytes: 2048},
		FailedKeys: []FailedKey{
			{Key: "a.txt", Reason: "AccessDenied"},
			{Key: "b/c.bin", VersionId: "v1", Reason: "InternalError"},
		},
	}
	if err := WriteArtifacts(dir, req, res); err != nil {
		t.Fatalf("WriteArtifacts: %v", err)
	}

	csvBytes, err := os.ReadFile(filepath.Join(dir, "failures.csv"))
	if err != nil {
		t.Fatalf("read failures.csv: %v", err)
	}
	csvStr := string(csvBytes)
	for _, want := range []string{"Key,VersionId,Reason", "a.txt", "b/c.bin", "v1", "AccessDenied"} {
		if !strings.Contains(csvStr, want) {
			t.Errorf("failures.csv missing %q. content=%s", want, csvStr)
		}
	}

	mBytes, err := os.ReadFile(filepath.Join(dir, "metrics.json"))
	if err != nil {
		t.Fatalf("read metrics.json: %v", err)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(mBytes, &m); err != nil {
		t.Fatalf("metrics.json unmarshal: %v", err)
	}
	if m["deleted"].(float64) != 42 {
		t.Errorf("metrics.deleted: got %v want 42", m["deleted"])
	}
	if m["bucket"].(string) != "my-bucket" {
		t.Errorf("metrics.bucket: got %v", m["bucket"])
	}
	if m["engine"].(string) != "sdk" {
		t.Errorf("metrics.engine: got %v", m["engine"])
	}
	if m["dryRun"].(bool) != true {
		t.Errorf("metrics.dryRun: got %v", m["dryRun"])
	}
	if m["totalObjects"].(float64) != 100 {
		t.Errorf("metrics.totalObjects: got %v", m["totalObjects"])
	}
}

func TestWriteArtifacts_EmptyDirSkips(t *testing.T) {
	// Empty outDir should be a no-op (no error, no files).
	if err := WriteArtifacts("", Request{}, Result{}); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}

func TestClassifyBucketError(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"404", "operation error S3: HeadBucket, https response error StatusCode: 404, RequestID: x, NotFound: ", "not found"},
		{"403", "https response error StatusCode: 403, AccessDenied", "access denied"},
		{"301", "https response error StatusCode: 301, PermanentRedirect", "different region"},
		{"dns", "dial tcp: lookup s3.bad.example: no such host", "cannot reach endpoint"},
		{"x509", "x509: certificate signed by unknown authority", "tls certificate error"},
		{"generic", "boom", "not accessible"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg := classifyBucketError("my-bucket", &fakeErr{tc.in})
			if !strings.Contains(strings.ToLower(msg), tc.want) {
				t.Errorf("got %q, want substring %q", msg, tc.want)
			}
		})
	}
}

type fakeErr struct{ s string }

func (e *fakeErr) Error() string { return e.s }

func TestWriteArtifacts_NoFailuresOmitsCSV(t *testing.T) {
	dir := t.TempDir()
	res := Result{Deleted: 5}
	if err := WriteArtifacts(dir, Request{Bucket: "b"}, res); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "failures.csv")); err == nil {
		t.Error("failures.csv should not exist when there are no failures")
	}
	if _, err := os.Stat(filepath.Join(dir, "metrics.json")); err != nil {
		t.Errorf("metrics.json should exist: %v", err)
	}
}
