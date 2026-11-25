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
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"sync"
)

func (o GetObjectOptions) parallelGetEnabled() bool {
	return o.ParallelWorkers > 1 && o.PartNumber == 0 && !o.Checksum && o.RDMABuffer == nil && o.headers["Range"] == ""
}

func parallelGetChunkSize(chunkSize, objectSize int64) int64 {
	chunkSize = max(chunkSize, minPartSize)
	if (objectSize+chunkSize-1)/chunkSize > defaultMaxPartsCount {
		chunkSize = (objectSize + defaultMaxPartsCount - 1) / defaultMaxPartsCount
	}
	return chunkSize
}

type parallelGetReader struct {
	*io.PipeReader
	cancel context.CancelFunc
}

func (r parallelGetReader) Close() error {
	r.cancel()
	return r.PipeReader.Close()
}

// getObjectParallel reads the object with concurrent range GETs pinned to
// its ETag, falling back to a single GET when it fits in one chunk or has
// no ETag.
func (c *Client) getObjectParallel(ctx context.Context, bucketName, objectName string, opts GetObjectOptions) (io.ReadCloser, ObjectInfo, error) {
	objectInfo, err := c.StatObject(ctx, bucketName, objectName, opts)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	chunkSize := parallelGetChunkSize(opts.ParallelChunkSize, objectInfo.Size)
	if objectInfo.Size <= chunkSize || objectInfo.ETag == "" {
		body, objectInfo, _, err := c.getObject(ctx, bucketName, objectName, opts)
		return body, objectInfo, err
	}

	// The caller keeps reusing opts.headers for later requests.
	opts.headers = maps.Clone(opts.headers)
	opts.SetMatchETag(objectInfo.ETag)

	ctx, cancel := context.WithCancel(ctx)
	pr, pw := io.Pipe()
	go func() {
		pw.CloseWithError(c.parallelGet(ctx, bucketName, objectName, opts, objectInfo.Size, chunkSize, pw))
	}()
	return parallelGetReader{PipeReader: pr, cancel: cancel}, objectInfo, nil
}

// parallelGet writes the object to w in order. At most opts.ParallelWorkers
// chunks are being fetched or held in memory at any time.
func (c *Client) parallelGet(ctx context.Context, bucketName, objectName string, opts GetObjectOptions, size, chunkSize int64, w io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type chunk struct {
		data []byte
		err  error
	}
	numChunks := (size + chunkSize - 1) / chunkSize
	pending := make(chan chan chunk, min(int64(opts.ParallelWorkers), numChunks)-1)
	go func() {
		defer close(pending)
		for start := int64(0); start < size; start += chunkSize {
			res := make(chan chunk, 1)
			select {
			case pending <- res:
			case <-ctx.Done():
				return
			}
			go func() {
				data, err := c.getObjectChunk(ctx, bucketName, objectName, opts, start, min(start+chunkSize, size)-1, size)
				res <- chunk{data: data, err: err}
			}()
		}
	}()

	for res := range pending {
		var r chunk
		select {
		case r = <-res:
		case <-ctx.Done():
			return ctx.Err()
		}
		if r.err != nil {
			return r.err
		}
		if _, err := w.Write(r.data); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (c *Client) getObjectChunk(ctx context.Context, bucketName, objectName string, opts GetObjectOptions, start, end, size int64) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, end-start+1))
	if err := c.getObjectChunkTo(ctx, bucketName, objectName, opts, start, end, size, buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// getObjectChunkTo streams the range [start, end] of the object into w,
// verifying the Content-Range and that exactly end-start+1 bytes arrive.
func (c *Client) getObjectChunkTo(ctx context.Context, bucketName, objectName string, opts GetObjectOptions, start, end, size int64, w io.Writer) error {
	opts.headers = maps.Clone(opts.headers)
	if err := opts.SetRange(start, end); err != nil {
		return err
	}
	body, _, header, err := c.getObject(ctx, bucketName, objectName, opts)
	if err != nil {
		return err
	}
	defer body.Close()

	if cr := header.Get("Content-Range"); cr != fmt.Sprintf("bytes %d-%d/%d", start, end, size) {
		return fmt.Errorf("unexpected Content-Range %q for range %d-%d", cr, start, end)
	}
	// io.CopyN only returns a nil error when exactly end-start+1 bytes
	// were copied; a short body surfaces as io.ErrUnexpectedEOF.
	if _, err := io.CopyN(w, body, end-start+1); err != nil {
		return err
	}
	return nil
}

// fGetObjectParallel downloads the object with concurrent range GETs into
// per-range shard files under a temporary directory unique to this call,
// merges the shards in order, verifies the merged object, and renames it
// into place. The ranges are contiguous and non-overlapping and cover the
// whole object. Any failure or cancellation removes the whole temporary
// directory, so an interrupted download leaves no partial output or orphan
// shard files behind and never touches existing files.
func (c *Client) fGetObjectParallel(ctx context.Context, bucketName, objectName, filePath string, opts GetObjectOptions, objectStat ObjectInfo) error {
	size := objectStat.Size
	chunkSize := parallelGetChunkSize(opts.ParallelChunkSize, size)
	numParts := (size + chunkSize - 1) / chunkSize

	// A temporary directory unique to this download: concurrent downloads
	// of the same object never share files, and removing the directory
	// cleans up everything this run created and nothing else.
	tmpDir, err := os.MkdirTemp(filepath.Dir(filePath), "."+filepath.Base(filePath)+".*.minio")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	// Pin every range GET to the ETag seen at Stat time. The caller keeps
	// reusing opts.headers for later requests.
	opts.headers = maps.Clone(opts.headers)
	opts.SetMatchETag(objectStat.ETag)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	partPath := func(i int64) string {
		return filepath.Join(tmpDir, fmt.Sprintf("part-%05d", i))
	}

	// Fetch each range into its own shard file. The first failure cancels
	// the remaining ranges.
	var wg sync.WaitGroup
	sem := make(chan struct{}, opts.ParallelWorkers)
	errCh := make(chan error, 1)
	for i := int64(0); i < numParts; i++ {
		start := i * chunkSize
		end := min(start+chunkSize, size) - 1
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			part, err := os.OpenFile(partPath(i), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
			if err == nil {
				err = c.getObjectChunkTo(ctx, bucketName, objectName, opts, start, end, size, part)
				if cerr := part.Close(); err == nil {
					err = cerr
				}
			}
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// Merge the shards in range order; since the ranges are contiguous and
	// non-overlapping, concatenation places every byte at its offset.
	mergedPath := filepath.Join(tmpDir, "merged")
	merged, err := os.OpenFile(mergedPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}

	// Hash the merged object while merging when the server advertised a
	// full-object checksum for it.
	var verifier *checksumVerifyingReader
	if objectStat.ChecksumMode == ChecksumFullObjectMode.String() {
		if verifier = c.newChecksumVerifyingReader(objectStat); verifier != nil {
			defer verifier.Close()
		}
	}
	w := io.Writer(merged)
	if verifier != nil {
		w = io.MultiWriter(merged, verifier)
	}

	var written int64
	for i := int64(0); i < numParts; i++ {
		var part *os.File
		part, err = os.Open(partPath(i))
		if err != nil {
			_ = merged.Close()
			return err
		}
		var n int64
		n, err = io.Copy(w, part)
		_ = part.Close()
		written += n
		if err != nil {
			_ = merged.Close()
			return err
		}
	}
	if err = merged.Close(); err != nil {
		return err
	}

	if written != size {
		return fmt.Errorf("parallel download of %s/%s: merged %d bytes, want %d", bucketName, objectName, written, size)
	}
	if verifier != nil {
		if err = verifier.VerifyChecksum(); err != nil {
			return err
		}
	}

	// Safely completed. Now commit by renaming to the actual filename.
	return os.Rename(mergedPath, filePath)
}
