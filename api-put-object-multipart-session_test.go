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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	testMPPartSize = 5 << 20
	testMPObjectSz = 15 << 20 // exactly three testMPPartSize parts
	testMPUploadID = "upload-id"
)

// mpHandlerConfig steers the mock S3 server used by the cleanup tests.
type mpHandlerConfig struct {
	mu sync.Mutex
	// failParts maps part numbers to the HTTP status code every UploadPart
	// request for that part must return.
	failParts map[int]int
	// blockParts blocks every UploadPart request for those part numbers
	// until its request context is canceled (or the server shuts down).
	blockParts map[int]bool
	// abortStatus is the status code AbortMultipartUpload returns; 204 by
	// default.
	abortStatus int
	// abortNoSuchUpload makes the abort return 404 NoSuchUpload, which the
	// client treats as a successful cleanup.
	abortNoSuchUpload bool
}

func (cfg *mpHandlerConfig) setFail(part, status int) {
	cfg.mu.Lock()
	cfg.failParts[part] = status
	cfg.mu.Unlock()
}

func (cfg *mpHandlerConfig) setBlock(part int) {
	cfg.mu.Lock()
	cfg.blockParts[part] = true
	cfg.mu.Unlock()
}

func (cfg *mpHandlerConfig) setAbortStatus(status int) {
	cfg.mu.Lock()
	cfg.abortStatus = status
	cfg.mu.Unlock()
}

// mpCounters records what the mock server actually did.
type mpCounters struct {
	initiates atomic.Int32
	puts      atomic.Int32
	completes atomic.Int32
	aborts    atomic.Int32

	mu       sync.Mutex
	partPuts map[int]int // part number -> UploadPart requests observed
	abortIDs []string
}

func newMPTestServer(t *testing.T, cfg *mpHandlerConfig) (*Client, *mpCounters) {
	t.Helper()
	counters := &mpCounters{partPuts: make(map[int]int)}
	srv := newTestStatClient(t, func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		switch {
		case r.Method == http.MethodPost && q.Has("uploads"):
			counters.initiates.Add(1)
			w.Header().Set("Content-Type", "application/xml")
			io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?>`+
				`<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>object</Key>`+
				"<UploadId>"+testMPUploadID+"</UploadId></InitiateMultipartUploadResult>")
		case r.Method == http.MethodPut:
			part, _ := strconv.Atoi(q.Get("partNumber"))
			io.Copy(io.Discard, r.Body)
			counters.puts.Add(1)
			counters.mu.Lock()
			counters.partPuts[part]++
			counters.mu.Unlock()

			cfg.mu.Lock()
			status, fail := cfg.failParts[part]
			blocked := cfg.blockParts[part]
			cfg.mu.Unlock()
			if blocked {
				// Stay in flight until the client gives up on this part.
				<-r.Context().Done()
				return
			}
			if fail {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(status)
				fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code>`+
					`<Message>mock part failure</Message></Error>`, InternalError)
				return
			}
			w.Header().Set("ETag", `"etag"`)
		case r.Method == http.MethodPost && q.Get("uploadId") != "":
			io.Copy(io.Discard, r.Body)
			counters.completes.Add(1)
			w.Header().Set("Content-Type", "application/xml")
			io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?>`+
				`<CompleteMultipartUploadResult><Bucket>bucket</Bucket><Key>object</Key>`+
				`<ETag>&quot;etag-3&quot;</ETag></CompleteMultipartUploadResult>`)
		case r.Method == http.MethodDelete && q.Get("uploadId") != "":
			io.Copy(io.Discard, r.Body)
			cfg.mu.Lock()
			status := cfg.abortStatus
			noSuchUpload := cfg.abortNoSuchUpload
			cfg.mu.Unlock()
			if status == 0 {
				status = http.StatusNoContent
			}
			counters.aborts.Add(1)
			counters.mu.Lock()
			counters.abortIDs = append(counters.abortIDs, q.Get("uploadId"))
			counters.mu.Unlock()
			if noSuchUpload {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusNotFound)
				io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchUpload</Code>`+
					`<Message>The specified multipart upload does not exist.</Message></Error>`)
				return
			}
			if status != http.StatusNoContent {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(status)
				io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>InternalError</Code>`+
					`<Message>mock abort failure</Message></Error>`)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	})
	// Retries stay cheap and deterministic in count.
	srv.maxRetries = 2
	return srv, counters
}

func newMPConfig() *mpHandlerConfig {
	return &mpHandlerConfig{
		failParts:   make(map[int]int),
		blockParts:  make(map[int]bool),
		abortStatus: http.StatusNoContent,
	}
}

func plainReader() io.Reader {
	// The anonymous wrapper hides ReadAt so PutObject takes the sequential
	// streaming path.
	return struct{ io.Reader }{bytes.NewReader(make([]byte, testMPObjectSz))}
}

// TestMPCleanupRetryExhaustionAborts: when a part's retries are exhausted the
// upload aborts, no later part is dispatched, and the error carries the
// upload ID plus the parts that made it to the server.
func TestMPCleanupRetryExhaustionAborts(t *testing.T) {
	cfg := newMPConfig()
	cfg.setFail(2, http.StatusServiceUnavailable)
	clnt, counters := newMPTestServer(t, cfg)

	_, err := clnt.PutObject(t.Context(), "bucket", "object", plainReader(), testMPObjectSz,
		PutObjectOptions{PartSize: testMPPartSize})
	if err == nil {
		t.Fatal("PutObject succeeded, want a part failure")
	}
	if code := ToErrorResponse(err).Code; code != InternalError {
		t.Fatalf("error code = %q, want %q (err: %v)", code, InternalError, err)
	}
	info, ok := AsMultipartUploadError(err)
	if !ok {
		t.Fatalf("error is not a multipart upload error: %v", err)
	}
	if info.UploadID() != testMPUploadID {
		t.Errorf("UploadID = %q, want %q", info.UploadID(), testMPUploadID)
	}
	if info.AbortFailed() {
		t.Errorf("AbortFailed = true, want false: %v", info.AbortError())
	}
	if got, want := info.Parts(), []int{1}; !equalInts(got, want) {
		t.Errorf("uploaded parts = %v, want %v", got, want)
	}
	if n := counters.completes.Load(); n != 0 {
		t.Errorf("complete requests = %d, want 0", n)
	}
	if n := counters.aborts.Load(); n != 1 {
		t.Errorf("abort requests = %d, want 1", n)
	}
	counters.mu.Lock()
	part2Attempts := counters.partPuts[2]
	part3Attempts := counters.partPuts[3]
	counters.mu.Unlock()
	if part2Attempts != clnt.maxRetries {
		t.Errorf("part 2 attempts = %d, want %d", part2Attempts, clnt.maxRetries)
	}
	if part3Attempts != 0 {
		t.Errorf("part 3 was dispatched %d times, want 0: the loop must stop at the failed part", part3Attempts)
	}
}

// TestMPCleanupCancelStopsRemainingParts: a caller cancellation stops the
// remaining parts, the abort still goes out on a detached context, and the
// returned error is distinguishable as a cancellation while still carrying
// the multipart upload state.
func TestMPCleanupCancelStopsRemainingParts(t *testing.T) {
	cfg := newMPConfig()
	// Part 2 never returns on its own; the test cancels while it is in flight.
	cfg.setBlock(2)
	clnt, counters := newMPTestServer(t, cfg)

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	_, err := clnt.PutObject(ctx, "bucket", "object", plainReader(), testMPObjectSz,
		PutObjectOptions{PartSize: testMPPartSize})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	info, ok := AsMultipartUploadError(err)
	if !ok {
		t.Fatalf("cancellation error lost the multipart upload state: %v", err)
	}
	if info.UploadID() != testMPUploadID {
		t.Errorf("UploadID = %q, want %q", info.UploadID(), testMPUploadID)
	}
	if got, want := info.Parts(), []int{1}; !equalInts(got, want) {
		t.Errorf("uploaded parts = %v, want %v", got, want)
	}
	if n := counters.completes.Load(); n != 0 {
		t.Errorf("complete requests = %d, want 0", n)
	}
	if n := counters.aborts.Load(); n != 1 {
		t.Errorf("abort requests = %d, want 1 (the abort must ignore the canceled context)", n)
	}
	counters.mu.Lock()
	part3Attempts := counters.partPuts[3]
	counters.mu.Unlock()
	if part3Attempts != 0 {
		t.Errorf("part 3 was dispatched %d times, want 0", part3Attempts)
	}
}

// TestMPCleanupAbortFailureKeepsUploadID: when the abort itself fails, the
// upload stays open, the returned error carries the upload ID and the abort
// error, and a later retry of the abort succeeds and is then idempotent.
func TestMPCleanupAbortFailureKeepsUploadID(t *testing.T) {
	cfg := newMPConfig()
	cfg.setFail(2, http.StatusServiceUnavailable)
	cfg.setAbortStatus(http.StatusInternalServerError)
	clnt, counters := newMPTestServer(t, cfg)
	clnt.maxRetries = 1 // a single abort attempt, no retry wait

	_, err := clnt.PutObject(t.Context(), "bucket", "object", plainReader(), testMPObjectSz,
		PutObjectOptions{PartSize: testMPPartSize})
	info, ok := AsMultipartUploadError(err)
	if !ok {
		t.Fatalf("error is not a multipart upload error: %v", err)
	}
	if !info.AbortFailed() || info.AbortError() == nil {
		t.Fatalf("AbortFailed = %v, abortErr = %v; want a failed abort", info.AbortFailed(), info.AbortError())
	}
	if info.UploadID() != testMPUploadID {
		t.Errorf("UploadID = %q, want %q", info.UploadID(), testMPUploadID)
	}
	if n := counters.aborts.Load(); n != 1 {
		t.Errorf("abort requests = %d, want 1", n)
	}

	// The upload is still open. Retry the cleanup; this time the mock
	// accepts the abort.
	cfg.setAbortStatus(http.StatusNoContent)
	upload := &multipartUpload{
		client:    clnt,
		bucket:    "bucket",
		object:    "object",
		uploadID:  testMPUploadID,
		state:     uploadOpen,
		abortErr:  nil,
		partsSeen: make(map[int]struct{}),
	}
	if err := upload.abort(t.Context(), errors.New("retry cleanup")); err == nil {
		t.Fatal("abort after a successful cleanup must still return the annotated cause, got nil")
	}
	if n := counters.aborts.Load(); n != 2 {
		t.Errorf("abort requests = %d, want 2", n)
	}
	// A further abort is a no-op: the upload is finished.
	if err := upload.abort(t.Context(), errors.New("retry cleanup")); err != nil {
		t.Fatalf("third abort = %v, want nil no-op", err)
	}
	if n := counters.aborts.Load(); n != 2 {
		t.Errorf("abort requests = %d, want still 2", n)
	}
}

// TestMPCleanupParallelBatchCancel: in the parallel uploader a single part
// failure cancels the rest of the batch, the parts that did reach the server
// remain traceable, and exactly one abort reclaims them.
func TestMPCleanupParallelBatchCancel(t *testing.T) {
	cfg := newMPConfig()
	cfg.setFail(2, http.StatusServiceUnavailable)
	clnt, counters := newMPTestServer(t, cfg)

	_, err := clnt.PutObject(t.Context(), "bucket", "object",
		bytes.NewReader(make([]byte, testMPObjectSz)), testMPObjectSz, PutObjectOptions{
			PartSize:              testMPPartSize,
			ConcurrentStreamParts: true,
			NumThreads:            3,
		})
	if err == nil {
		t.Fatal("PutObject succeeded, want a part failure")
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v; a part failure must not be reported as a caller cancellation", err)
	}
	info, ok := AsMultipartUploadError(err)
	if !ok {
		t.Fatalf("error is not a multipart upload error: %v", err)
	}
	// Parts 1 and 3 were dispatched in the same batch and could finish before
	// the cancellation propagated; both numbers must remain traceable.
	got := info.Parts()
	if !containsInt(got, 1) || !containsInt(got, 3) {
		t.Errorf("uploaded parts = %v, want it to contain 1 and 3", got)
	}
	if containsInt(got, 2) {
		t.Errorf("uploaded parts = %v; the failed part 2 must not be recorded", got)
	}
	if n := counters.completes.Load(); n != 0 {
		t.Errorf("complete requests = %d, want 0", n)
	}
	if n := counters.aborts.Load(); n != 1 {
		t.Errorf("abort requests = %d, want 1", n)
	}
}

// TestMPCleanupReadAtBatchCancel: the ReadAt parallel uploader cancels every
// in-flight worker when the caller gives up, without a complete request.
func TestMPCleanupReadAtBatchCancel(t *testing.T) {
	cfg := newMPConfig()
	cfg.setBlock(2)
	cfg.setBlock(3)
	clnt, counters := newMPTestServer(t, cfg)

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	_, err := clnt.PutObject(ctx, "bucket", "object",
		bytes.NewReader(make([]byte, testMPObjectSz)), testMPObjectSz,
		PutObjectOptions{PartSize: testMPPartSize, NumThreads: 3})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if n := counters.completes.Load(); n != 0 {
		t.Errorf("complete requests = %d, want 0", n)
	}
	if n := counters.aborts.Load(); n != 1 {
		t.Errorf("abort requests = %d, want 1", n)
	}
	if info, ok := AsMultipartUploadError(err); !ok || info.UploadID() != testMPUploadID {
		t.Fatalf("error lost the multipart upload state: %v", err)
	}
}

// TestMPSessionFinalizeIdempotent drives the session directly: the
// completing and aborting request is each sent at most once, and repeating
// either finalize is a no-op.
func TestMPSessionFinalizeIdempotent(t *testing.T) {
	t.Run("complete then abort", func(t *testing.T) {
		clnt, counters := newMPTestServer(t, newMPConfig())
		upload, err := clnt.newMultipartUpload(t.Context(), "bucket", "object", PutObjectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		complete := completeMultipartUpload{Parts: []CompletePart{{PartNumber: 1, ETag: "etag"}}}
		if _, err := upload.complete(t.Context(), complete, PutObjectOptions{}); err != nil {
			t.Fatalf("first complete = %v", err)
		}
		// Second complete: no new request.
		if _, err := upload.complete(t.Context(), complete, PutObjectOptions{}); err != nil {
			t.Fatalf("second complete = %v, want nil no-op", err)
		}
		// Abort after completion: no request either.
		if err := upload.abort(t.Context(), errors.New("late failure")); err != nil {
			t.Fatalf("abort after completion = %v, want nil no-op", err)
		}
		if n := counters.completes.Load(); n != 1 {
			t.Errorf("complete requests = %d, want 1", n)
		}
		if n := counters.aborts.Load(); n != 0 {
			t.Errorf("abort requests = %d, want 0", n)
		}
	})

	t.Run("abort then complete", func(t *testing.T) {
		clnt, counters := newMPTestServer(t, newMPConfig())
		upload, err := clnt.newMultipartUpload(t.Context(), "bucket", "object", PutObjectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		cause := errors.New("part failed")
		if err := upload.abort(t.Context(), cause); err == nil {
			t.Fatal("first abort returned nil, want the annotated cause")
		}
		// Second abort: no-op.
		if err := upload.abort(t.Context(), cause); err != nil {
			t.Fatalf("second abort = %v, want nil no-op", err)
		}
		// Complete after abort: no request either.
		if _, err := upload.complete(t.Context(), completeMultipartUpload{}, PutObjectOptions{}); err != nil {
			t.Fatalf("complete after abort = %v, want nil no-op", err)
		}
		if n := counters.aborts.Load(); n != 1 {
			t.Errorf("abort requests = %d, want 1", n)
		}
		if n := counters.completes.Load(); n != 0 {
			t.Errorf("complete requests = %d, want 0", n)
		}
	})
}

// TestMPCleanupHappyPathNoAbort makes sure the successful path neither
// aborts nor double-completes.
func TestMPCleanupHappyPathNoAbort(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		name := "sequential"
		opts := PutObjectOptions{PartSize: testMPPartSize}
		reader := plainReader()
		if parallel {
			name = "parallel"
			opts.ConcurrentStreamParts = true
			opts.NumThreads = 3
			reader = bytes.NewReader(make([]byte, testMPObjectSz))
		}
		t.Run(name, func(t *testing.T) {
			clnt, counters := newMPTestServer(t, newMPConfig())
			info, err := clnt.PutObject(t.Context(), "bucket", "object", reader, testMPObjectSz, opts)
			if err != nil {
				t.Fatal(err)
			}
			if info.Size != testMPObjectSz {
				t.Errorf("Size = %d, want %d", info.Size, testMPObjectSz)
			}
			if n := counters.completes.Load(); n != 1 {
				t.Errorf("complete requests = %d, want 1", n)
			}
			if n := counters.aborts.Load(); n != 0 {
				t.Errorf("abort requests = %d, want 0", n)
			}
		})
	}
}

// TestMPCleanupNoSuchUploadIsSuccess: an abort that races with a server-side
// lifecycle cleanup and gets NoSuchUpload must be treated as success, not as
// a failed cleanup that keeps the upload ID dangling.
func TestMPCleanupNoSuchUploadIsSuccess(t *testing.T) {
	cfg := newMPConfig()
	cfg.setFail(2, http.StatusServiceUnavailable)
	cfg.abortNoSuchUpload = true
	clnt, counters := newMPTestServer(t, cfg)

	_, err := clnt.PutObject(t.Context(), "bucket", "object", plainReader(), testMPObjectSz,
		PutObjectOptions{PartSize: testMPPartSize})
	info, ok := AsMultipartUploadError(err)
	if !ok {
		t.Fatalf("error is not a multipart upload error: %v", err)
	}
	if info.AbortFailed() {
		t.Fatalf("AbortFailed = true for a NoSuchUpload response: %v", info.AbortError())
	}
	if n := counters.aborts.Load(); n != 1 {
		t.Errorf("abort requests = %d, want 1", n)
	}
	// The underlying cause is still the part failure, not the abort.
	if code := ToErrorResponse(err).Code; code != InternalError {
		t.Errorf("error code = %q, want %q", code, InternalError)
	}
}

// TestMPCleanupCancellationDistinguishable asserts the same failure scenario
// reports either a cancellation or an upload failure depending on the cause,
// and both shapes keep the upload ID reachable.
func TestMPCleanupCancellationDistinguishable(t *testing.T) {
	t.Run("caller cancellation", func(t *testing.T) {
		cfg := newMPConfig()
		cfg.setBlock(2)
		clnt, _ := newMPTestServer(t, cfg)

		ctx, cancel := context.WithCancel(t.Context())
		go func() {
			time.Sleep(150 * time.Millisecond)
			cancel()
		}()
		_, err := clnt.PutObject(ctx, "bucket", "object", plainReader(), testMPObjectSz,
			PutObjectOptions{PartSize: testMPPartSize})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("errors.Is(err, context.Canceled) = false for %v", err)
		}
		if errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("a plain cancel must not look like a deadline: %v", err)
		}
		if _, ok := AsMultipartUploadError(err); !ok {
			t.Fatalf("cancellation lost the upload ID: %v", err)
		}
	})

	t.Run("upload failure", func(t *testing.T) {
		cfg := newMPConfig()
		cfg.setFail(1, http.StatusServiceUnavailable)
		clnt, _ := newMPTestServer(t, cfg)

		_, err := clnt.PutObject(t.Context(), "bucket", "object", plainReader(), testMPObjectSz,
			PutObjectOptions{PartSize: testMPPartSize})
		if errors.Is(err, context.Canceled) {
			t.Fatalf("an upload failure must not look like a caller cancellation: %v", err)
		}
		if code := ToErrorResponse(err).Code; code != InternalError {
			t.Fatalf("error code = %q, want %q (err: %v)", code, InternalError, err)
		}
	})
}

// TestMPSessionConcurrentFinalize hammers complete and abort concurrently: the
// state machine must guarantee at most one finishing request reaches the
// server, whichever wins.
func TestMPSessionConcurrentFinalize(t *testing.T) {
	clnt, counters := newMPTestServer(t, newMPConfig())
	upload, err := clnt.newMultipartUpload(t.Context(), "bucket", "object", PutObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	complete := completeMultipartUpload{Parts: []CompletePart{{PartNumber: 1, ETag: "etag"}}}

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = upload.complete(t.Context(), complete, PutObjectOptions{})
		}()
	}
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = upload.abort(t.Context(), errors.New("concurrent abort"))
		}()
	}
	wg.Wait()

	if n := counters.completes.Load() + counters.aborts.Load(); n != 1 {
		t.Fatalf("finishing requests = %d (completes=%d, aborts=%d), want exactly 1",
			n, counters.completes.Load(), counters.aborts.Load())
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func containsInt(xs []int, want int) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}
