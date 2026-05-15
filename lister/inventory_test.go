package lister

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// fakeS3 is an in-memory S3API implementation. It supports prefix filtering
// and pagination so the tests exercise the real code paths.
type fakeS3 struct {
	objects        []types.Object        // for ListObjectsV2
	versions       []types.ObjectVersion // for ListObjectVersions
	deleteMarkers  []types.DeleteMarkerEntry
	pageSize       int  // 0 → all in one page
	lsv2Calls      int
	versionsCalls  int
	lastLSV2Prefix string
}

func (f *fakeS3) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	f.lsv2Calls++
	f.lastLSV2Prefix = aws.ToString(in.Prefix)
	// Filter by prefix.
	var matching []types.Object
	for _, o := range f.objects {
		if in.Prefix == nil || strings.HasPrefix(aws.ToString(o.Key), *in.Prefix) {
			matching = append(matching, o)
		}
	}
	start := 0
	if in.ContinuationToken != nil {
		// token is the index encoded as a string
		_, _ = parseIntPanicOnErr(*in.ContinuationToken, &start)
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
	f.versionsCalls++
	var vers []types.ObjectVersion
	var dms []types.DeleteMarkerEntry
	for _, v := range f.versions {
		if in.Prefix == nil || strings.HasPrefix(aws.ToString(v.Key), *in.Prefix) {
			vers = append(vers, v)
		}
	}
	for _, dm := range f.deleteMarkers {
		if in.Prefix == nil || strings.HasPrefix(aws.ToString(dm.Key), *in.Prefix) {
			dms = append(dms, dm)
		}
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

func parseIntPanicOnErr(s string, out *int) (int, error) {
	n := 0
	for _, c := range s {
		n = n*10 + int(c-'0')
	}
	*out = n
	return n, nil
}

func obj(key string, size int64) types.Object {
	return types.Object{Key: aws.String(key), Size: aws.Int64(size)}
}

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
	if inv.TopLevelFolders != 2 { // logs, backups
		t.Errorf("TopLevelFolders: got %d want 2", inv.TopLevelFolders)
	}
	if inv.TotalSizeBytes != 10+100+200+50+1024 {
		t.Errorf("TotalSizeBytes: got %d", inv.TotalSizeBytes)
	}
	if fake.versionsCalls != 0 {
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
	// Top-level folder relative to prefix: 2024, 2025 → 2.
	if inv.TopLevelFolders != 2 {
		t.Errorf("TopLevelFolders under prefix: got %d want 2", inv.TopLevelFolders)
	}
	if inv.TotalSizeBytes != 100+200+50 {
		t.Errorf("TotalSizeBytes under prefix: got %d", inv.TotalSizeBytes)
	}
	if fake.lastLSV2Prefix != "logs/" {
		t.Errorf("S3 was called with prefix %q, want %q", fake.lastLSV2Prefix, "logs/")
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
	if fake.lsv2Calls != 3 { // 100 + 100 + 50
		t.Errorf("expected 3 ListObjectsV2 calls, got %d", fake.lsv2Calls)
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
		{"foo.txt", "", ""}, // root file, no folder
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
