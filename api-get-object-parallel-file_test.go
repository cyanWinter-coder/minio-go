/*
 * MinIO Go Library for Amazon S3 Compatible Cloud Storage
 * Copyright 2026 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package minio

import (
	"context"
	"encoding/base64"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

func (s *parallelGetServer) fGetObject(ctx context.Context, t *testing.T, filePath string, opts GetObjectOptions) error {
	clnt, err := New(s.Listener.Addr().String(), &Options{Region: "us-east-1"})
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	return clnt.FGetObject(ctx, "bucket", "object", filePath, opts)
}

// dirEntries returns the names of the entries in dir.
func dirEntries(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	slices.Sort(names)
	return names
}

// wantRanges verifies that the range GETs the server saw are contiguous,
// non-overlapping, and cover the whole object exactly once.
func (s *parallelGetServer) wantRanges(t *testing.T, size int64) {
	t.Helper()
	type rng struct{ start, end int64 }
	var ranges []rng
	for _, h := range s.getHeaders() {
		if r := h.Get("Range"); r != "" {
			var r2 rng
			if _, err := fmt.Sscanf(r, "bytes=%d-%d", &r2.start, &r2.end); err != nil {
				t.Fatalf("unparseable Range header %q", r)
			}
			ranges = append(ranges, r2)
		}
	}
	if len(ranges) < 2 {
		t.Fatalf("expected several range GETs, got %d", len(ranges))
	}
	slices.SortFunc(ranges, func(a, b rng) int { return int(a.start - b.start) })
	if ranges[0].start != 0 {
		t.Fatalf("first range starts at %d, want 0", ranges[0].start)
	}
	for i := 1; i < len(ranges); i++ {
		if ranges[i].start != ranges[i-1].end+1 {
			t.Fatalf("ranges %v and %v overlap or leave a gap", ranges[i-1], ranges[i])
		}
	}
	if last := ranges[len(ranges)-1]; last.end != size-1 {
		t.Fatalf("last range ends at %d, want %d", last.end, size-1)
	}
}

func TestFGetObjectParallel(t *testing.T) {
	s := newParallelGetServer(t, 3*minPartSize+123)
	dir := t.TempDir()
	dst := filepath.Join(dir, "out.bin")

	err := s.fGetObject(context.Background(), t, dst, GetObjectOptions{ParallelWorkers: 3})
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, s.data) {
		t.Fatalf("got %d bytes, want %d", len(got), len(s.data))
	}
	s.wantRanges(t, int64(len(s.data)))
	if names := dirEntries(t, dir); !slices.Equal(names, []string{"out.bin"}) {
		t.Fatalf("leftover files after download: %v", names)
	}
}

func TestFGetObjectParallelRangeFailure(t *testing.T) {
	s := newParallelGetServer(t, 3*minPartSize)
	s.failAt = minPartSize
	dir := t.TempDir()
	dst := filepath.Join(dir, "out.bin")

	err := s.fGetObject(context.Background(), t, dst, GetObjectOptions{ParallelWorkers: 2})
	if err == nil {
		t.Fatal("expected an error for a failed range")
	}
	if names := dirEntries(t, dir); len(names) != 0 {
		t.Fatalf("leftover files after failed download: %v", names)
	}
}

func TestFGetObjectParallelShortChunk(t *testing.T) {
	s := newParallelGetServer(t, 3*minPartSize)
	s.shortAt = minPartSize
	dir := t.TempDir()
	dst := filepath.Join(dir, "out.bin")

	err := s.fGetObject(context.Background(), t, dst, GetObjectOptions{ParallelWorkers: 2})
	if err == nil {
		t.Fatal("expected an error for a short chunk")
	}
	if names := dirEntries(t, dir); len(names) != 0 {
		t.Fatalf("leftover files after failed download: %v", names)
	}
}

func TestFGetObjectParallelCancel(t *testing.T) {
	s := newParallelGetServer(t, 3*minPartSize)
	s.delay = 500 * time.Millisecond
	dir := t.TempDir()
	dst := filepath.Join(dir, "out.bin")

	original := []byte("pre-existing content")
	if err := os.WriteFile(dst, original, 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	err := s.fGetObject(ctx, t, dst, GetObjectOptions{ParallelWorkers: 2})
	if err == nil {
		t.Fatal("expected an error after cancellation")
	}

	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, original) {
		t.Fatal("cancellation clobbered the existing destination file")
	}
	if names := dirEntries(t, dir); !slices.Equal(names, []string{"out.bin"}) {
		t.Fatalf("leftover files after canceled download: %v", names)
	}
}

func TestFGetObjectParallelConcurrentSameObject(t *testing.T) {
	s := newParallelGetServer(t, 2*minPartSize+1)
	dir := t.TempDir()

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, name := range []string{"a.bin", "b.bin"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = s.fGetObject(context.Background(), t, filepath.Join(dir, name), GetObjectOptions{ParallelWorkers: 2})
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("download %d: %v", i, err)
		}
	}
	for _, name := range []string{"a.bin", "b.bin"} {
		got, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, s.data) {
			t.Fatalf("%s: got %d bytes, want %d", name, len(got), len(s.data))
		}
	}
	if names := dirEntries(t, dir); !slices.Equal(names, []string{"a.bin", "b.bin"}) {
		t.Fatalf("leftover files after concurrent downloads: %v", names)
	}
}

func TestFGetObjectParallelRepeated(t *testing.T) {
	s := newParallelGetServer(t, 2*minPartSize+1)
	dir := t.TempDir()
	dst := filepath.Join(dir, "out.bin")

	for i := range 2 {
		if err := s.fGetObject(context.Background(), t, dst, GetObjectOptions{ParallelWorkers: 2}); err != nil {
			t.Fatalf("download %d: %v", i, err)
		}
		got, err := os.ReadFile(dst)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, s.data) {
			t.Fatalf("download %d: got %d bytes, want %d", i, len(got), len(s.data))
		}
	}
	if names := dirEntries(t, dir); !slices.Equal(names, []string{"out.bin"}) {
		t.Fatalf("leftover files after repeated downloads: %v", names)
	}
}

func crc32Base64(data []byte) string {
	sum := crc32.ChecksumIEEE(data)
	return base64.StdEncoding.EncodeToString([]byte{byte(sum >> 24), byte(sum >> 16), byte(sum >> 8), byte(sum)})
}

func TestFGetObjectParallelChecksum(t *testing.T) {
	s := newParallelGetServer(t, 2*minPartSize+1)
	s.checksumCRC32 = crc32Base64(s.data)
	dir := t.TempDir()
	dst := filepath.Join(dir, "out.bin")

	if err := s.fGetObject(context.Background(), t, dst, GetObjectOptions{ParallelWorkers: 2}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, s.data) {
		t.Fatalf("got %d bytes, want %d", len(got), len(s.data))
	}
	if names := dirEntries(t, dir); !slices.Equal(names, []string{"out.bin"}) {
		t.Fatalf("leftover files after download: %v", names)
	}
}

func TestFGetObjectParallelChecksumMismatch(t *testing.T) {
	s := newParallelGetServer(t, 2*minPartSize+1)
	// Advertise the checksum of a different object.
	s.checksumCRC32 = crc32Base64([]byte("not the object"))
	dir := t.TempDir()
	dst := filepath.Join(dir, "out.bin")

	err := s.fGetObject(context.Background(), t, dst, GetObjectOptions{ParallelWorkers: 2})
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("expected a checksum mismatch error, got %v", err)
	}
	if names := dirEntries(t, dir); len(names) != 0 {
		t.Fatalf("leftover files after checksum mismatch: %v", names)
	}
}
