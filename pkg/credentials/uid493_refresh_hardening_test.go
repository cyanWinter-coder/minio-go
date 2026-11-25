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

package credentials

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// uid493ScriptedProvider is a thread-safe provider whose expiration state is
// independent of retrieval success or failure.
type uid493ScriptedProvider struct {
	mu      sync.Mutex
	calls   atomic.Int64
	creds   Value
	err     error
	expired bool
}

func (p *uid493ScriptedProvider) Retrieve() (Value, error) {
	return p.RetrieveWithCredContext(defaultCredContext)
}

func (p *uid493ScriptedProvider) RetrieveWithCredContext(_ *CredContext) (Value, error) {
	p.calls.Add(1)
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.creds, p.err
}

func (p *uid493ScriptedProvider) IsExpired() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.expired
}

func (p *uid493ScriptedProvider) set(creds Value, err error, expired bool) {
	p.mu.Lock()
	p.creds, p.err, p.expired = creds, err, expired
	p.mu.Unlock()
}

// uid493BlockingProvider blocks retrieval until release is closed or the
// caller context ends. The result is read only after the wait, so tests can
// publish it by closing release.
type uid493BlockingProvider struct {
	started     chan struct{}
	startOnce   sync.Once
	release     chan struct{}
	releaseOnce sync.Once

	calls   atomic.Int64
	creds   Value
	err     error
	expired atomic.Bool
}

func (p *uid493BlockingProvider) Retrieve() (Value, error) {
	return p.RetrieveWithCredContext(&CredContext{Context: context.Background()})
}

func (p *uid493BlockingProvider) RetrieveWithCredContext(cc *CredContext) (Value, error) {
	p.calls.Add(1)
	if p.started != nil {
		p.startOnce.Do(func() { close(p.started) })
	}

	if p.release != nil {
		ctx := cc.requestContext()
		select {
		case <-p.release:
		case <-ctx.Done():
			return Value{}, ctx.Err()
		}
	}
	if p.err == nil {
		// Successful built-in providers update their own expiry state.
		p.expired.Store(false)
	}
	return p.creds, p.err
}

func (p *uid493BlockingProvider) IsExpired() bool {
	return p.expired.Load()
}

func (p *uid493BlockingProvider) free() {
	if p.release != nil {
		p.releaseOnce.Do(func() { close(p.release) })
	}
}

func uid493WaitChannel[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

func uid493WaitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	uid493WaitChannel(t, ch, what)
}

func uid493WaitForWaiters(t *testing.T, creds *Credentials, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		creds.Lock()
		got := creds.waiters
		creds.Unlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	creds.Lock()
	got := creds.waiters
	creds.Unlock()
	t.Fatalf("timed out waiting for %d credential waiters, have %d", want, got)
}

func TestUID493_ChainRetrieveAggregatesAllProviderErrors(t *testing.T) {
	firstErr := errors.New("FirstError")
	secondErr := errors.New("SecondError")
	chain := &Chain{Providers: []Provider{
		&credProvider{err: firstErr},
		&testCredProvider{err: secondErr},
	}}

	_, err := chain.RetrieveWithCredContext(defaultCredContext)
	if err == nil {
		t.Fatal("Expected an aggregate error when every provider fails")
	}

	var aggregate *AggregateError
	if !errors.As(err, &aggregate) {
		t.Fatalf("Expected *AggregateError, got %T: %v", err, err)
	}
	if len(aggregate.Errors) != 2 {
		t.Fatalf("Expected two provider errors, got %d: %v", len(aggregate.Errors), aggregate.Errors)
	}
	if !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
		t.Fatalf("Aggregate did not expose provider errors: %v", err)
	}
	if !errors.Is(err, ErrNoValidProviders) {
		t.Fatalf("Aggregate did not identify as ErrNoValidProviders: %v", err)
	}
	if got, want := err.Error(), "No valid providers found [FirstError SecondError]"; got != want {
		t.Fatalf("Error string mismatch:\ngot:  %q\nwant: %q", got, want)
	}
}

func TestUID493_ChainRetrieveEmptyProviderListIsRecognizable(t *testing.T) {
	_, err := (&Chain{}).RetrieveWithCredContext(defaultCredContext)
	if err == nil {
		t.Fatal("Expected an error for an empty provider chain")
	}
	if !errors.Is(err, ErrNoValidProviders) {
		t.Fatalf("Expected ErrNoValidProviders, got %v", err)
	}
	if got, want := err.Error(), "No valid providers found []"; got != want {
		t.Fatalf("Error string mismatch:\ngot:  %q\nwant: %q", got, want)
	}
}

func TestUID493_CredentialsConcurrentRefreshSharesOneResult(t *testing.T) {
	now := time.Now()
	oldCreds := Value{
		AccessKeyID:     "old-access",
		SecretAccessKey: "old-secret",
		Expiration:      now.Add(time.Hour),
	}
	newCreds := Value{
		AccessKeyID:     "new-access",
		SecretAccessKey: "new-secret",
		SessionToken:    "new-token",
		Expiration:      now.Add(2 * time.Hour),
	}

	provider := &uid493BlockingProvider{creds: oldCreds}
	creds := New(provider)
	creds.CurrentTime = func() time.Time { return now }

	got, err := creds.GetWithContext(defaultCredContext)
	if err != nil {
		t.Fatal(err)
	}
	if got.AccessKeyID != oldCreds.AccessKeyID {
		t.Fatalf("initial credentials = %q, want %q", got.AccessKeyID, oldCreds.AccessKeyID)
	}

	provider.started = make(chan struct{})
	provider.release = make(chan struct{})
	provider.expired.Store(true)
	provider.creds = newCreds
	t.Cleanup(provider.free)

	const callers = 32
	start := make(chan struct{})
	results := make(chan Value, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			var v Value
			var err error
			if i%2 == 0 {
				v, err = creds.Get()
			} else {
				v, err = creds.GetWithContext(defaultCredContext)
			}
			if err != nil {
				errs <- err
				return
			}
			results <- v
		}(i)
	}

	close(start)
	uid493WaitSignal(t, provider.started, "the refresh to start")
	uid493WaitForWaiters(t, creds, callers-1)

	provider.free()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	uid493WaitSignal(t, done, "all concurrent Get calls")

	close(results)
	close(errs)
	for err := range errs {
		t.Errorf("concurrent Get failed: %v", err)
	}
	for v := range results {
		if v.AccessKeyID != newCreds.AccessKeyID || v.SessionToken != newCreds.SessionToken {
			t.Fatalf("caller observed %+v, want the shared new credentials %+v", v, newCreds)
		}
	}

	if calls := provider.calls.Load(); calls != 2 {
		t.Fatalf("provider retrievals = %d, want initial retrieval plus one shared refresh", calls)
	}
}

func TestUID493_CredentialsRefreshFailureFallsBackUntilHardExpiry(t *testing.T) {
	base := time.Now()
	expiration := base.Add(time.Hour)
	oldCreds := Value{
		AccessKeyID:     "stale-access",
		SecretAccessKey: "stale-secret",
		Expiration:      expiration,
	}
	refreshErr := errors.New("temporary credential service rate limited")
	provider := &uid493BlockingProvider{creds: oldCreds}

	now := base
	creds := New(provider)
	creds.CurrentTime = func() time.Time { return now }

	got, err := creds.GetWithContext(defaultCredContext)
	if err != nil {
		t.Fatal(err)
	}
	if got.AccessKeyID != oldCreds.AccessKeyID {
		t.Fatalf("initial AccessKeyID = %q, want %q", got.AccessKeyID, oldCreds.AccessKeyID)
	}

	// Provider enters its proactive refresh window, the refresh blocks, and
	// then fails.
	provider.started = make(chan struct{})
	provider.release = make(chan struct{})
	provider.err = refreshErr
	provider.expired.Store(true)
	t.Cleanup(provider.free)

	// Concurrent failures must be one shared provider call and all callers
	// must receive the still-valid cached value.
	const callers = 16
	var wg sync.WaitGroup
	errCh := make(chan error, callers)
	valCh := make(chan Value, callers)
	start := make(chan struct{})
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			v, err := creds.GetWithContext(defaultCredContext)
			if err != nil {
				errCh <- err
				return
			}
			valCh <- v
		}()
	}
	close(start)
	uid493WaitSignal(t, provider.started, "the failed refresh to start")
	uid493WaitForWaiters(t, creds, callers-1)
	provider.free()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	uid493WaitSignal(t, done, "concurrent fallback calls")
	close(errCh)
	close(valCh)
	for err := range errCh {
		t.Errorf("expected stale credentials instead of error, got %v", err)
	}
	for v := range valCh {
		if v.AccessKeyID != oldCreds.AccessKeyID {
			t.Fatalf("fallback AccessKeyID = %q, want %q", v.AccessKeyID, oldCreds.AccessKeyID)
		}
	}
	if calls := provider.calls.Load(); calls != 2 {
		t.Fatalf("provider calls after concurrent failed refresh = %d, want 2", calls)
	}
	if !errors.Is(creds.LastRefreshError(), refreshErr) {
		t.Fatalf("LastRefreshError = %v, want %v", creds.LastRefreshError(), refreshErr)
	}

	// Boundary against the real Expiration, not the proactive provider window:
	// the exact expiration instant is still accepted by Before.
	now = expiration
	got, err = creds.GetWithContext(defaultCredContext)
	if err != nil {
		t.Fatalf("credentials should remain usable at the expiration boundary: %v", err)
	}
	if got.AccessKeyID != oldCreds.AccessKeyID {
		t.Fatalf("boundary AccessKeyID = %q, want %q", got.AccessKeyID, oldCreds.AccessKeyID)
	}

	// Immediately after the real expiration the cached value is dead and the
	// refresh error must surface.
	now = expiration.Add(time.Nanosecond)
	_, err = creds.GetWithContext(defaultCredContext)
	if !errors.Is(err, refreshErr) {
		t.Fatalf("error after hard expiration = %v, want %v", err, refreshErr)
	}
}

func TestUID493_CredentialsLateOldRefreshResultDoesNotOverwriteNewer(t *testing.T) {
	provider := &uid493ScriptedProvider{}
	creds := New(provider)
	newer := Value{AccessKeyID: "newer", SecretAccessKey: "secret", Expiration: time.Now().Add(time.Hour)}
	older := Value{AccessKeyID: "older", SecretAccessKey: "secret", Expiration: time.Now().Add(30 * time.Minute)}

	oldCall := &credentialRefresh{done: make(chan struct{}), sequence: 1}
	newCall := &credentialRefresh{done: make(chan struct{}), sequence: 2}
	creds.refreshSequence = 2

	// Complete the newer attempt first, then the delayed older attempt.
	creds.finishRefresh(newCall, newer, nil, nil, false)
	creds.finishRefresh(oldCall, older, nil, nil, false)

	got, err := creds.GetWithContext(defaultCredContext)
	if err != nil {
		t.Fatal(err)
	}
	if got.AccessKeyID != "newer" {
		t.Fatalf("cached AccessKeyID = %q, want late old result %q to be discarded", got.AccessKeyID, "newer")
	}
}

func TestUID493_ExpiryLeadWindowBoundary(t *testing.T) {
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	expiration := base.Add(time.Hour)
	lead := 5 * time.Minute

	for _, tc := range []struct {
		name string
		now  time.Time
		want bool
	}{
		{name: "before lead", now: expiration.Add(-lead).Add(-time.Nanosecond), want: false},
		{name: "exact lead boundary", now: expiration.Add(-lead), want: false},
		{name: "after lead boundary", now: expiration.Add(-lead).Add(time.Nanosecond), want: true},
		{name: "at expiration", now: expiration, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var e Expiry
			current := tc.now
			e.CurrentTime = func() time.Time { return current }
			e.SetExpiration(expiration, lead)
			if got := e.IsExpired(); got != tc.want {
				t.Fatalf("IsExpired() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestUID493_ExpiryDefaultRatioWindowBoundary(t *testing.T) {
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	lifetime := 100 * time.Minute
	expiration := base.Add(lifetime)
	effectiveExpiration := base.Add(80 * time.Minute)

	for _, tc := range []struct {
		name string
		now  time.Time
		want bool
	}{
		{name: "before 80 percent boundary", now: effectiveExpiration.Add(-time.Nanosecond), want: false},
		{name: "exact 80 percent boundary", now: effectiveExpiration, want: false},
		{name: "after 80 percent boundary", now: effectiveExpiration.Add(time.Nanosecond), want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var e Expiry
			current := base
			e.CurrentTime = func() time.Time { return current }
			e.SetExpiration(expiration, DefaultExpiryWindow)
			current = tc.now
			if got := e.IsExpired(); got != tc.want {
				t.Fatalf("IsExpired() at %s = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

func TestUID493_CredentialsExplicitAndContextGetsShareCache(t *testing.T) {
	provider := &uid493ScriptedProvider{
		creds: Value{AccessKeyID: "shared", SecretAccessKey: "secret"},
	}
	creds := New(provider)

	v1, err := creds.Get()
	if err != nil {
		t.Fatal(err)
	}
	v2, err := creds.GetWithContext(&CredContext{Context: context.Background()})
	if err != nil {
		t.Fatal(err)
	}
	if v1 != v2 {
		t.Fatalf("explicit Get = %+v and GetWithContext = %+v differ", v1, v2)
	}
	if calls := provider.calls.Load(); calls != 1 {
		t.Fatalf("provider calls = %d, want one cached retrieval", calls)
	}

	creds.Expire()
	provider.set(Value{AccessKeyID: "rotated", SecretAccessKey: "secret"}, nil, false)
	v3, err := creds.Get()
	if err != nil {
		t.Fatal(err)
	}
	v4, err := creds.GetWithContext(defaultCredContext)
	if err != nil {
		t.Fatal(err)
	}
	if v3 != v4 || v3.AccessKeyID != "rotated" {
		t.Fatalf("rotated values mismatch: v3=%+v v4=%+v", v3, v4)
	}
	if calls := provider.calls.Load(); calls != 2 {
		t.Fatalf("provider calls after Expire = %d, want 2", calls)
	}
}
