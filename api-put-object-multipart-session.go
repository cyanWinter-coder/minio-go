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
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Lifecycle states of a multipart upload.
const (
	uploadOpen       = iota // parts may still be uploaded
	uploadCompleting        // CompleteMultipartUpload is in flight or has been sent
	uploadCompleted         // CompleteMultipartUpload succeeded
	uploadAborted           // AbortMultipartUpload succeeded, or NoSuchUpload proved it is gone
)

// multipartUploadError wraps every failure that leaves or ends a multipart
// upload. It carries the upload ID and the part numbers that had already
// been uploaded before the failure. When the automatic abort itself fails,
// AbortFailed is true and the upload ID is the handle a caller needs to
// retry the cleanup out of band.
//
// Callers distinguish a caller-side cancellation from an upload failure
// with errors.Is(err, context.Canceled) (or context.DeadlineExceeded): the
// original error is always preserved by Unwrap.
type multipartUploadError struct {
	cause       error
	uploadID    string
	parts       []int // successfully uploaded part numbers, ascending
	abortFailed bool  // the automatic abort did not remove the upload
	abortErr    error // error returned by the failed abort
}

func (e *multipartUploadError) Error() string {
	var b strings.Builder
	b.WriteString(e.cause.Error())
	fmt.Fprintf(&b, " (multipart upload %q", e.uploadID)
	if len(e.parts) > 0 {
		fmt.Fprintf(&b, ", %d part(s) uploaded: %v", len(e.parts), e.parts)
	}
	if e.abortFailed {
		b.WriteString(", abort failed: ")
		b.WriteString(e.abortErr.Error())
		b.WriteString("; the upload ID is retained so the cleanup can be retried")
	}
	b.WriteString(")")
	return b.String()
}

func (e *multipartUploadError) Unwrap() error { return e.cause }

// Is keeps errors.Is working against the wrapped cause, in particular
// context.Canceled, context.DeadlineExceeded and reader errors.
func (e *multipartUploadError) Is(target error) bool {
	switch target {
	case context.Canceled, context.DeadlineExceeded:
		return errors.Is(e.cause, target)
	}
	return false
}

// withUpload annotates err with the multipart upload's state. An error that
// already carries upload state is returned unchanged so annotations do not
// nest.
func withUpload(err error, u *multipartUpload) error {
	if err == nil || u == nil {
		return err
	}
	var mue *multipartUploadError
	if errors.As(err, &mue) {
		return err
	}
	return &multipartUploadError{
		cause:    err,
		uploadID: u.uploadID,
		parts:    u.uploadedParts(),
	}
}

// multipartUpload tracks the lifecycle of one S3 multipart upload. Every
// PutObject/ComposeObject multipart path holds one and finishes the upload
// through complete or abort. Each finishing request is sent at most once:
// repeating either operation after the upload has been completed or aborted
// is a no-op that never touches the server again.
type multipartUpload struct {
	client   *Client
	bucket   string
	object   string
	uploadID string

	mu    sync.Mutex
	state int // one of uploadOpen/uploadCompleted/uploadAborted

	// abortErr is set when the automatic abort failed. It stays set so a
	// later retry of abort can report the same failed cleanup.
	abortErr error

	parts     []int // part numbers known to have been uploaded, ascending
	partsSeen map[int]struct{}
}

// newMultipartUpload initiates a multipart upload and returns its tracker.
// The initiation request itself happens before any part exists, so its
// error is returned unwrapped: there is no upload ID to clean up when it
// fails.
func (c *Client) newMultipartUpload(ctx context.Context, bucketName, objectName string, opts PutObjectOptions) (*multipartUpload, error) {
	uploadID, err := c.newUploadID(ctx, bucketName, objectName, opts)
	if err != nil {
		return nil, err
	}
	return &multipartUpload{
		client:    c,
		bucket:    bucketName,
		object:    objectName,
		uploadID:  uploadID,
		partsSeen: make(map[int]struct{}),
	}, nil
}

// UploadID exposes the S3 upload identifier.
func (u *multipartUpload) UploadID() string { return u.uploadID }

// recordPart records a part number that was uploaded successfully. Safe for
// concurrent use; duplicates are ignored so retried parts show up once.
func (u *multipartUpload) recordPart(partNumber int) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if _, ok := u.partsSeen[partNumber]; ok {
		return
	}
	u.partsSeen[partNumber] = struct{}{}
	u.parts = append(u.parts, partNumber)
	sort.Ints(u.parts)
}

// uploadedParts returns the successfully uploaded part numbers, ascending.
func (u *multipartUpload) uploadedParts() []int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]int(nil), u.parts...)
}

// complete sends CompleteMultipartUpload. It is idempotent: a second call
// after a successful completion, or after an abort, is a no-op without a
// second request. When the complete request fails, the upload is left open
// so the caller's failure path (or an explicit abort) reclaims the parts;
// the returned error already carries the upload ID and uploaded parts.
func (u *multipartUpload) complete(ctx context.Context, complete completeMultipartUpload, opts PutObjectOptions) (UploadInfo, error) {
	u.mu.Lock()
	if u.state != uploadOpen {
		u.mu.Unlock()
		// A request was already sent, or the upload was finished: never send
		// a second one.
		return UploadInfo{}, nil
	}
	// Claim the completion while holding the lock so a concurrent abort or
	// another complete cannot issue a second request.
	u.state = uploadCompleting
	u.mu.Unlock()

	info, err := u.client.completeMultipartUpload(ctx, u.bucket, u.object, u.uploadID, complete, opts)

	u.mu.Lock()
	defer u.mu.Unlock()
	if err == nil {
		u.state = uploadCompleted
		return info, nil
	}
	// The complete request failed: reopen the state so the caller's failure
	// path can abort and reclaim the already uploaded parts. The error
	// already carries the upload ID and the uploaded part numbers.
	u.state = uploadOpen
	return UploadInfo{}, withUpload(err, u)
}

// abort sends AbortMultipartUpload unless the upload was already finished.
// It is idempotent: after a successful abort or completion it is a no-op
// without a second request, and in that case it returns nil. The abort is
// detached from ctx so it is still delivered when the caller canceled the
// context. When the abort fails, the state stays open, the upload ID is
// retained and the returned error carries both cause and the abort error so
// the cleanup can be retried.
func (u *multipartUpload) abort(ctx context.Context, cause error) error {
	u.mu.Lock()
	if u.state != uploadOpen {
		u.mu.Unlock()
		// Completed or already aborted: nothing left to do.
		return nil
	}
	if cause == nil {
		cause = context.Canceled
	}
	aborted, abortErr := u.sendAbortLocked(ctx)
	if aborted {
		u.state = uploadAborted
		u.mu.Unlock()
		// Cleanup succeeded: surface the original failure annotated with the
		// upload ID and the parts that were reclaimed.
		return withUpload(cause, u)
	}
	u.abortErr = abortErr
	err := u.annotateLocked(cause)
	u.mu.Unlock()
	return err
}

// annotateLocked wraps cause with the upload state and, when the automatic
// abort failed, the abort error. Caller must hold u.mu.
func (u *multipartUpload) annotateLocked(cause error) error {
	mue := &multipartUploadError{
		cause:    cause,
		uploadID: u.uploadID,
		parts:    append([]int(nil), u.parts...),
	}
	if u.abortErr != nil {
		mue.abortFailed = true
		mue.abortErr = u.abortErr
	}
	return mue
}

// sendAbortLocked performs the actual AbortMultipartUpload HTTP request. It
// detaches from the caller's context so a canceled or expired PutObject
// context does not prevent the cleanup; NoSuchUpload is treated as success
// because the storage end has already reclaimed the parts. Caller must hold
// u.mu.
func (u *multipartUpload) sendAbortLocked(ctx context.Context) (bool, error) {
	abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
	defer cancel()
	err := u.client.abortMultipartUpload(abortCtx, u.bucket, u.object, u.uploadID)
	if err != nil {
		if ToErrorResponse(err).Code == NoSuchUpload {
			return true, nil
		}
		return false, err
	}
	return true, nil
}

// MultipartUploadError lets callers inspect a failed multipart upload. The
// upload ID stays accessible after PutObject returns so an abort that failed
// can be retried out of band, for example with
// Client.RemoveIncompleteUpload or Core.AbortMultipartUpload.
type MultipartUploadError struct {
	err *multipartUploadError
}

// UploadID is the identifier of the multipart upload that is still open when
// AbortFailed is true.
func (e MultipartUploadError) UploadID() string { return e.err.uploadID }

// Parts lists the part numbers that were uploaded before the failure.
func (e MultipartUploadError) Parts() []int { return append([]int(nil), e.err.parts...) }

// AbortFailed reports whether the automatic AbortMultipartUpload request
// failed. When true, the upload and its parts still exist on the storage end.
func (e MultipartUploadError) AbortFailed() bool { return e.err.abortFailed }

// AbortError returns the error of the failed abort, or nil.
func (e MultipartUploadError) AbortError() error { return e.err.abortErr }

// AsMultipartUploadError returns the multipart upload details attached to
// err, if any.
func AsMultipartUploadError(err error) (MultipartUploadError, bool) {
	var mue *multipartUploadError
	if !errors.As(err, &mue) {
		return MultipartUploadError{}, false
	}
	return MultipartUploadError{err: mue}, true
}
