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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7/pkg/credentials"
)

type uid493SharedCacheProvider struct {
	started chan struct{}
	once    sync.Once
	release chan struct{}

	calls   atomic.Int64
	expired atomic.Bool
}

func (p *uid493SharedCacheProvider) Retrieve() (credentials.Value, error) {
	return p.RetrieveWithCredContext(&credentials.CredContext{Context: context.Background()})
}

func (p *uid493SharedCacheProvider) RetrieveWithCredContext(cc *credentials.CredContext) (credentials.Value, error) {
	p.calls.Add(1)
	p.once.Do(func() { close(p.started) })

	ctx := context.Background()
	if cc != nil && cc.Context != nil {
		ctx = cc.Context
	}
	select {
	case <-p.release:
	case <-ctx.Done():
		return credentials.Value{}, ctx.Err()
	}

	// A successful retrieval refreshes the provider's own expiry state.
	p.expired.Store(false)
	return credentials.Value{
		AccessKeyID:     "shared-access",
		SecretAccessKey: "shared-secret",
		SignerType:      credentials.SignatureV4,
	}, nil
}

func (p *uid493SharedCacheProvider) IsExpired() bool {
	return p.expired.Load()
}

func (p *uid493SharedCacheProvider) free() {
	select {
	case <-p.release:
	default:
		close(p.release)
	}
}

// TestUID493_ExplicitAndImplicitCredentialsShareCache verifies that Client.GetCreds
// and credentials obtained internally by an S3 operation join the same
// Credentials cache/refresh instead of independently calling the provider.
func TestUID493_ExplicitAndImplicitCredentialsShareCache(t *testing.T) {
	provider := &uid493SharedCacheProvider{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	provider.expired.Store(true)
	defer provider.free()

	clnt := newTestClient(t, credentials.New(provider), &recordingTransport{})

	type explicitResult struct {
		value credentials.Value
		err   error
	}
	explicitCh := make(chan explicitResult, 1)
	go func() {
		value, err := clnt.GetCreds()
		explicitCh <- explicitResult{value: value, err: err}
	}()

	recvSignal(t, provider.started, "explicit credential retrieval to start")

	requestErrCh := make(chan error, 1)
	go func() {
		_, err := clnt.BucketExists(context.Background(), "shared-cache-bucket")
		requestErrCh <- err
	}()

	// Release without requiring the request goroutine to have reached the
	// cache: if it arrives before completion it joins the flight; if after,
	// the now-fresh cache serves it. Either path must not call the provider
	// again.
	provider.free()

	select {
	case res := <-explicitCh:
		if res.err != nil {
			t.Fatalf("GetCreds failed: %v", res.err)
		}
		if res.value.AccessKeyID != "shared-access" {
			t.Fatalf("GetCreds AccessKeyID = %q", res.value.AccessKeyID)
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("timed out waiting for explicit GetCreds")
	}

	if err := recvErr(t, requestErrCh, "implicit BucketExists credential use"); err != nil {
		t.Fatalf("BucketExists failed: %v", err)
	}
	if calls := provider.calls.Load(); calls != 1 {
		t.Fatalf("provider calls = %d, want explicit and implicit retrieval to share one cache refresh", calls)
	}
}
