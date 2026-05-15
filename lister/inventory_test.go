package lister

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// fakeS3 is an in-memory S3API implementation. It supports prefix filtering,
// the Delimiter parameter (with CommonPrefixes), pagination, and is safe for
// concurrent use by ParallelScan goroutines.
type fakeS3 struct {
	objects       []types.Object
	versions      []types.ObjectVersion
	deleteMarkers []types.DeleteMarkerEntry
	pageSize      int

	mu              sync.Mutex
	lsv2Calls       int64
	versionsCalls   int64
	delimiterCalls  int64
	prefixesQueried []string
}

func (f *fakeS3) recordPrefix(p string) {
	f.mu.Lock()
	f.prefixesQueried = append(f.prefixesQueried, p)
	f.mu.Unlock()
}

func (f *fakeS3) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	atomic.AddInt64(&f.lsv2Calls, 1)
	prefix := aws.ToString(in.Prefix)
	f.recordPrefix(prefix)
	delimiter := aws.ToString(in.Delimiter)

	var matching []types.Object
	commonSet := map[string]struct{}{}
	for _, o := range f.objects {
		key := aws.ToString(o.Key)
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if delimiter == "" {
			matching = append(matching, o)
			continue
		}
		rest := strings.TrimPrefix(key, prefix)
		if idx := strings.Index(rest, delimiter); idx >= 0 {
			commonSet[prefix+rest[:idx+len(delimiter)]] = struct{}{}
		} else {
			matching = append(matching, o)
		}
	}

	if delimiter != "" {
		atomic.AddInt64(&f.delimiterCalls, 1)
		var commons []types.CommonPrefix
		for p := range commonSet {
			pp := p
			commons = append(commons, types.CommonPrefix{Prefix: &pp})
		}
		return &s3.ListObjectsV2Output{
			Contents:       matching,
			CommonPrefixes: commons,
			IsTruncated:    aws.Bool(false),
		}, nil
	}

	start := 0
	if in.ContinuationToken != nil {
		start = parsePosInt(*in.ContinuationToken)
	}
	end := len(matching)
	if f.pageSize > 0 && start+f.pageSize < end {
		end = start + f.pageSize
	}
	out := &s3.ListObjectsV2Output{Contents: matching[start:end]}
	if end < len(matching) {
		out.IsTruncated = aws.Bool(true)
		tok := encodeInt(end)
		out.NextContinuationToken = &tok
	} else {
		out.IsTruncated = aws.Bool(false)
	}
	return out, nil
}

func (f *fakeS3) ListObjectVersions(_ context.Context, in *s3.ListObjectVersionsInput, _ ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error) {
	atomic.AddInt64(&f.versionsCalls, 1)
	prefix := aws.ToString(in.Prefix)
	delimiter := aws.ToString(in.Delimiter)

	var vers []types.ObjectVersion
	var dms []types.DeleteMarkerEntry
	for _, v := range f.versions {
		key := aws.ToString(v.Key)
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if delimiter != "" {
			rest := strings.TrimPrefix(key, prefix)
			if strings.Contains(rest, delimiter) {
				continue // belongs to a sub-prefix
			}
		}
		vers = append(vers, v)
	}
	for _, dm := range f.deleteMarkers {
		key := aws.ToString(dm.Key)
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if delimiter != "" {
			rest := strings.TrimPrefix(key, prefix)
			if strings.Contains(rest, delimiter) {
				continue
			}
		}
		dms = append(dms, dm)
	}
	return &s3.ListObjectVersionsOutput{
		Versions:      vers,
		DeleteMarkers: dms,
		IsTruncated:   aws.Bool(false),
	}, nil
}

func encodeInt(i int) string {
	const digits = "0123456789"
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{digits[i%10]}, b...)
		i /= 10
	}
	return string(b)
}

func parsePosInt(s string) int {
	n := 0
	for _, c := range s {
		n = n*10 + int(c-'0')
	}
	return n
}

func obj(key string, size int64) types.Object {
	return types.Object{Key: aws.String(key), Size: aws.Int64(size)}
}

// --------------------------- existing tests ---------------------------

func TestScan_Unversioned_CountsAndTopLevelFolders(t *testing.T) {
	fake := &fakeS3{
		objects: []types.Object{
			obj("a.txt", 10),
			obj("logs/2024/01.log", 100),
			obj("logs/2024/02.log", 200),
			obj("logs/2025/01.log", 50),
			obj("backups/db.tar", 1024),
		},
	}
	inv, err := Scan(context.Background(), fake, "my-bucket", "", false)
	if err != nil {
		t.Fatal(err)
	}
	if inv.TotalObjects != 5 {
		t.Errorf("TotalObjects: got %d want 5", inv.TotalObjects)
	}
	if inv.TopLevelFolders != 2 {
		t.Errorf("TopLevelFolders: got %d want 2", inv.TopLevelFolders)
	}
	if inv.TotalSizeBytes != 10+100+200+50+1024 {
		t.Errorf("TotalSizeBytes: got %d", inv.TotalSizeBytes)
	}
	if atomic.LoadInt64(&fake.versionsCalls) != 0 {
		t.Error("ListObjectVersions should not have been called for unversioned scan")
	}
}

func TestScan_WithPrefix_FiltersAndCountsCorrectly(t *testing.T) {
	fake := &fakeS3{
		objects: []types.Object{
			obj("a.txt", 10),
			obj("logs/2024/01.log", 100),
			obj("logs/2024/02.log", 200),
			obj("logs/2025/01.log", 50),
			obj("backups/db.tar", 1024),
		},
	}
	inv, err := Scan(context.Background(), fake, "my-bucket", "logs/", false)
	if err != nil {
		t.Fatal(err)
	}
	if inv.TotalObjects != 3 {
		t.Errorf("TotalObjects under prefix: got %d want 3", inv.TotalObjects)
	}
	if inv.TopLevelFolders != 2 {
		t.Errorf("TopLevelFolders under prefix: got %d want 2", inv.TopLevelFolders)
	}
	if inv.TotalSizeBytes != 100+200+50 {
		t.Errorf("TotalSizeBytes under prefix: got %d", inv.TotalSizeBytes)
	}
}

func TestScan_Pagination(t *testing.T) {
	var objs []types.Object
	for i := 0; i < 250; i++ {
		objs = append(objs, obj("k/"+encodeInt(i), 1))
	}
	fake := &fakeS3{objects: objs, pageSize: 100}
	inv, err := Scan(context.Background(), fake, "b", "", false)
	if err != nil {
		t.Fatal(err)
	}
	if inv.TotalObjects != 250 {
		t.Errorf("TotalObjects across pages: got %d want 250", inv.TotalObjects)
	}
	if atomic.LoadInt64(&fake.lsv2Calls) != 3 {
		t.Errorf("expected 3 ListObjectsV2 calls, got %d", atomic.LoadInt64(&fake.lsv2Calls))
	}
}

func TestScan_Versioned_CountsVersionsAndMarkers(t *testing.T) {
	fake := &fakeS3{
		objects: []types.Object{obj("a", 1)},
		versions: []types.ObjectVersion{
			{Key: aws.String("a"), VersionId: aws.String("v1")},
			{Key: aws.String("a"), VersionId: aws.String("v2")},
			{Key: aws.String("b"), VersionId: aws.String("v1")},
		},
		deleteMarkers: []types.DeleteMarkerEntry{
			{Key: aws.String("c"), VersionId: aws.String("v1")},
		},
	}
	inv, err := Scan(context.Background(), fake, "b", "", true)
	if err != nil {
		t.Fatal(err)
	}
	if inv.VersionedObjects != 3 {
		t.Errorf("VersionedObjects: got %d want 3", inv.VersionedObjects)
	}
	if inv.DeleteMarkers != 1 {
		t.Errorf("DeleteMarkers: got %d want 1", inv.DeleteMarkers)
	}
}

func TestTopLevelSegment(t *testing.T) {
	cases := []struct {
		key, prefix, want string
	}{
		{"foo/bar.txt", "", "foo"},
		{"foo.txt", "", ""},
		{"logs/2024/x.log", "logs/", "2024"},
		{"logs/file.log", "logs/", ""},
		{"a/b/c/d", "", "a"},
	}
	for _, tc := range cases {
		got := topLevelSegment(tc.key, tc.prefix)
		if got != tc.want {
			t.Errorf("topLevelSegment(%q,%q) = %q, want %q", tc.key, tc.prefix, got, tc.want)
		}
	}
}

// --------------------------- ParallelScan tests ---------------------------

func TestParallelScan_AutoUsesDelimiter(t *testing.T) {
	// 6 top-level folders → auto threshold (4) triggers delimiter strategy.
	fake := &fakeS3{
		objects: []types.Object{
			obj("a/1", 10), obj("a/2", 10),
			obj("b/1", 20),
			obj("c/1", 30),
			obj("d/1", 40),
			obj("e/1", 50),
			obj("f/1", 60),
			obj("root.txt", 5),
		},
	}
	var progressCalls int64
	inv, err := ParallelScan(context.Background(), fake, "bucket", "", false, ScanOptions{
		Concurrency: 4,
		Strategy:    StrategyAuto,
		OnProgress:  func(ScanProgress) { atomic.AddInt64(&progressCalls, 1) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if inv.TotalObjects != 8 {
		t.Errorf("TotalObjects: got %d want 8", inv.TotalObjects)
	}
	if inv.TopLevelFolders != 6 {
		t.Errorf("TopLevelFolders: got %d want 6", inv.TopLevelFolders)
	}
	if inv.TotalSizeBytes != 10+10+20+30+40+50+60+5 {
		t.Errorf("TotalSizeBytes: got %d", inv.TotalSizeBytes)
	}
	if atomic.LoadInt64(&fake.delimiterCalls) == 0 {
		t.Error("auto strategy with many folders should have issued at least one delimiter call")
	}
	if atomic.LoadInt64(&progressCalls) == 0 {
		t.Error("OnProgress was never called")
	}
}

func TestParallelScan_AutoFallsBackToSharded(t *testing.T) {
	// Only 2 sub-prefixes — below the threshold of 4 → falls back to sharded.
	fake := &fakeS3{
		objects: []types.Object{
			obj("alpha/1", 1),
			obj("beta/1", 2),
			obj("zeta.txt", 3),
		},
	}
	inv, err := ParallelScan(context.Background(), fake, "bucket", "", false, ScanOptions{
		Concurrency: 4,
		Strategy:    StrategyAuto,
	})
	if err != nil {
		t.Fatal(err)
	}
	if inv.TotalObjects != 3 {
		t.Errorf("TotalObjects: got %d want 3", inv.TotalObjects)
	}
	if inv.TopLevelFolders != 2 {
		t.Errorf("TopLevelFolders: got %d want 2", inv.TopLevelFolders)
	}
	if inv.TotalSizeBytes != 6 {
		t.Errorf("TotalSizeBytes: got %d want 6", inv.TotalSizeBytes)
	}
	// Sharded mode issues 256 list calls plus the 1 discovery call.
	calls := atomic.LoadInt64(&fake.lsv2Calls)
	if calls < 200 {
		t.Errorf("expected sharded fallback to issue many list calls, got %d", calls)
	}
}

func TestParallelScan_DelimiterExplicit(t *testing.T) {
	fake := &fakeS3{
		objects: []types.Object{
			obj("logs/2024/a", 1),
			obj("logs/2025/b", 2),
			obj("data/c", 3),
			obj("rootkey", 4),
		},
	}
	inv, err := ParallelScan(context.Background(), fake, "bucket", "", false, ScanOptions{
		Concurrency: 4,
		Strategy:    StrategyDelimiter,
	})
	if err != nil {
		t.Fatal(err)
	}
	if inv.TotalObjects != 4 {
		t.Errorf("TotalObjects: got %d want 4", inv.TotalObjects)
	}
	if inv.TopLevelFolders != 2 { // logs, data
		t.Errorf("TopLevelFolders: got %d want 2", inv.TopLevelFolders)
	}
}

func TestParallelScan_ShardedExplicit(t *testing.T) {
	fake := &fakeS3{
		objects: []types.Object{
			obj("aaa.txt", 1),
			obj("bbb.txt", 2),
			obj("ccc.txt", 3),
		},
	}
	inv, err := ParallelScan(context.Background(), fake, "bucket", "", false, ScanOptions{
		Concurrency: 8,
		Strategy:    StrategySharded,
	})
	if err != nil {
		t.Fatal(err)
	}
	if inv.TotalObjects != 3 {
		t.Errorf("TotalObjects: got %d want 3", inv.TotalObjects)
	}
	if inv.TopLevelFolders != 0 { // no '/' in any key → no folders
		t.Errorf("TopLevelFolders: got %d want 0", inv.TopLevelFolders)
	}
	if inv.TotalSizeBytes != 6 {
		t.Errorf("TotalSizeBytes: got %d want 6", inv.TotalSizeBytes)
	}
}

func TestParallelScan_SerialFallback(t *testing.T) {
	fake := &fakeS3{
		objects: []types.Object{obj("x", 1)},
	}
	// Concurrency=1 forces serial.
	inv, err := ParallelScan(context.Background(), fake, "bucket", "", false, ScanOptions{
		Concurrency: 1,
		Strategy:    StrategyAuto,
	})
	if err != nil {
		t.Fatal(err)
	}
	if inv.TotalObjects != 1 {
		t.Errorf("TotalObjects: got %d want 1", inv.TotalObjects)
	}
	// Serial path never uses the delimiter call.
	if atomic.LoadInt64(&fake.delimiterCalls) != 0 {
		t.Error("serial path should not have issued a delimiter call")
	}
}

func TestParallelScan_VersionedDelimiter(t *testing.T) {
	fake := &fakeS3{
		objects: []types.Object{
			obj("a/1", 1), obj("b/1", 1), obj("c/1", 1), obj("d/1", 1), obj("e/1", 1),
		},
		versions: []types.ObjectVersion{
			{Key: aws.String("a/1"), VersionId: aws.String("v1")},
			{Key: aws.String("a/1"), VersionId: aws.String("v2")},
			{Key: aws.String("b/1"), VersionId: aws.String("v1")},
			{Key: aws.String("c/1"), VersionId: aws.String("v1")},
			{Key: aws.String("d/1"), VersionId: aws.String("v1")},
			{Key: aws.String("e/1"), VersionId: aws.String("v1")},
		},
		deleteMarkers: []types.DeleteMarkerEntry{
			{Key: aws.String("a/old"), VersionId: aws.String("v1")},
		},
	}
	inv, err := ParallelScan(context.Background(), fake, "bucket", "", true, ScanOptions{
		Concurrency: 4,
		Strategy:    StrategyAuto, // 5 folders → delimiter
	})
	if err != nil {
		t.Fatal(err)
	}
	if inv.VersionedObjects != 6 {
		t.Errorf("VersionedObjects: got %d want 6", inv.VersionedObjects)
	}
	if inv.DeleteMarkers != 1 {
		t.Errorf("DeleteMarkers: got %d want 1", inv.DeleteMarkers)
	}
}

func TestParallelScan_WithPrefix_DelimiterCountsRelativeFolders(t *testing.T) {
	fake := &fakeS3{
		objects: []types.Object{
			obj("logs/2024/jan.log", 10),
			obj("logs/2024/feb.log", 20),
			obj("logs/2025/jan.log", 30),
			obj("logs/2026/jan.log", 40),
			obj("logs/2027/jan.log", 50),
			obj("data/x.bin", 100), // outside prefix, should be ignored
		},
	}
	inv, err := ParallelScan(context.Background(), fake, "bucket", "logs/", false, ScanOptions{
		Concurrency: 4,
		Strategy:    StrategyAuto,
	})
	if err != nil {
		t.Fatal(err)
	}
	if inv.TotalObjects != 5 {
		t.Errorf("TotalObjects under prefix: got %d want 5", inv.TotalObjects)
	}
	if inv.TopLevelFolders != 4 { // 2024, 2025, 2026, 2027
		t.Errorf("TopLevelFolders under prefix: got %d want 4", inv.TopLevelFolders)
	}
	if inv.TotalSizeBytes != 10+20+30+40+50 {
		t.Errorf("TotalSizeBytes under prefix: got %d", inv.TotalSizeBytes)
	}
}

// --------------------------- Benchmarks ---------------------------

// slowFakeS3 simulates real-world S3 latency per page (default 5ms). It also
// keeps the same Delimiter semantics as fakeS3.
type slowFakeS3 struct {
	fakeS3
	pageLatencyMs int
}

func (s *slowFakeS3) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if s.pageLatencyMs > 0 {
		time.Sleep(time.Duration(s.pageLatencyMs) * time.Millisecond)
	}
	return s.fakeS3.ListObjectsV2(ctx, in, opts...)
}

func (s *slowFakeS3) ListObjectVersions(ctx context.Context, in *s3.ListObjectVersionsInput, opts ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error) {
	if s.pageLatencyMs > 0 {
		time.Sleep(time.Duration(s.pageLatencyMs) * time.Millisecond)
	}
	return s.fakeS3.ListObjectVersions(ctx, in, opts...)
}

func BenchmarkScan_Serial_VsParallel(b *testing.B) {
	const numFolders = 20
	const keysPerFolder = 50
	var objs []types.Object
	for f := 0; f < numFolders; f++ {
		for k := 0; k < keysPerFolder; k++ {
			objs = append(objs, obj("folder"+encodeInt(f)+"/key"+encodeInt(k), 1))
		}
	}

	b.Run("serial", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			fake := &slowFakeS3{fakeS3: fakeS3{objects: objs, pageSize: 100}, pageLatencyMs: 2}
			if _, err := Scan(context.Background(), fake, "b", "", false); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("parallel_auto", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			fake := &slowFakeS3{fakeS3: fakeS3{objects: objs, pageSize: 100}, pageLatencyMs: 2}
			if _, err := ParallelScan(context.Background(), fake, "b", "", false, ScanOptions{
				Concurrency: 8, Strategy: StrategyAuto,
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
}
