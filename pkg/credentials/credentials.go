/*
 * MinIO Go Library for Amazon S3 Compatible Cloud Storage
 * Copyright 2017 MinIO, Inc.
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
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"time"
)

const (
	// STSVersion sts version string
	STSVersion = "2011-06-15"

	// How much duration to slash from the given expiration duration
	defaultExpiryWindow = 0.8
)

// defaultCredContext is used when the credential context doesn't
// actually matter or the default context is suitable.
var defaultCredContext = &CredContext{Client: http.DefaultClient}

// A Value is the S3 credentials value for individual credential fields.
type Value struct {
	// S3 Access key ID
	AccessKeyID string

	// S3 Secret Access Key
	SecretAccessKey string

	// S3 Session Token
	SessionToken string

	// Expiration of this credentials - null means no expiration associated
	Expiration time.Time

	// Signature Type.
	SignerType SignatureType
}

// A Provider is the interface for any component which will provide credentials
// Value. A provider is required to manage its own Expired state, and what to
// be expired means.
type Provider interface {
	// RetrieveWithCredContext returns nil if it successfully retrieved the
	// value. Error is returned if the value were not obtainable, or empty.
	// optionally takes CredContext for additional context to retrieve credentials.
	RetrieveWithCredContext(cc *CredContext) (Value, error)

	// Retrieve returns nil if it successfully retrieved the value.
	// Error is returned if the value were not obtainable, or empty.
	//
	// Deprecated: Retrieve() exists for historical compatibility and should not
	// be used. To get new credentials use the RetrieveWithCredContext function
	// to ensure the proper context (i.e. HTTP client) will be used.
	Retrieve() (Value, error)

	// IsExpired returns if the credentials are no longer valid, and need
	// to be retrieved.
	IsExpired() bool
}

// CredContext is passed to the Retrieve function of a provider to provide
// some additional context to retrieve credentials.
type CredContext struct {
	// Client specifies the HTTP client that should be used if an HTTP
	// request is to be made to fetch the credentials.
	Client *http.Client

	// Endpoint specifies the MinIO endpoint that will be used if no
	// explicit endpoint is provided.
	Endpoint string

	// Context is the optional caller context. Cancellation and deadlines
	// on it propagate to the HTTP requests the built-in providers make
	// to fetch credentials, in addition to any provider-level timeout
	// bound. A nil Context means no caller cancellation applies.
	Context context.Context
}

// requestContext returns the caller context carried by cc, or
// context.Background() when no Context is set.
func (cc *CredContext) requestContext() context.Context {
	if cc.Context == nil {
		return context.Background()
	}
	return cc.Context
}

// A Expiry provides shared expiration logic to be used by credentials
// providers to implement expiry functionality.
//
// The best method to use this struct is as an anonymous field within the
// provider's struct.
//
// Example:
//
//	type IAMCredentialProvider struct {
//	    Expiry
//	    ...
//	}
type Expiry struct {
	// The date/time when to expire on
	expiration time.Time

	// If set will be used by IsExpired to determine the current time.
	// Defaults to time.Now if CurrentTime is not set.
	CurrentTime func() time.Time
}

// SetExpiration sets the expiration IsExpired will check when called.
//
// If window is greater than 0 the expiration time will be reduced by the
// window value.
//
// Using a window is helpful to trigger credentials to expire sooner than
// the expiration time given to ensure no requests are made with expired
// tokens.
func (e *Expiry) SetExpiration(expiration time.Time, window time.Duration) {
	if e.CurrentTime == nil {
		e.CurrentTime = time.Now
	}
	cut := window
	if cut < 0 {
		expireIn := expiration.Sub(e.CurrentTime())
		cut = time.Duration(float64(expireIn) * (1 - defaultExpiryWindow))
	}
	e.expiration = expiration.Add(-cut)
}

// IsExpired returns if the credentials are expired.
func (e *Expiry) IsExpired() bool {
	if e.CurrentTime == nil {
		e.CurrentTime = time.Now
	}
	return e.expiration.Before(e.CurrentTime())
}

// Credentials - A container for synchronous safe retrieval of credentials Value.
// Credentials will cache the credentials value until they expire. Once the value
// expires the next Get will attempt to retrieve valid credentials.
//
// Credentials is safe to use across multiple goroutines and will manage the
// synchronous state so the Providers do not need to implement their own
// synchronization.
//
// The first Credentials.Get() will always call Provider.Retrieve() to get the
// first instance of the credentials Value. All calls to Get() after that
// will return the cached credentials Value until IsExpired() returns true.
//
// When the provider enters its refresh lead window, concurrent callers share
// one retrieval and its result. If that retrieval fails but the cached Value
// has a non-zero Expiration still in the future, the cached Value continues to
// be returned until its hard Expiration. Successful replacements are stored
// under the Credentials lock and ordered by refresh generation so a delayed
// older result cannot replace a newer one.
type Credentials struct {
	sync.Mutex

	creds        Value
	forceRefresh bool
	provider     Provider

	// inFlight is the single refresh shared by all concurrent callers. It is
	// never used to hold the lock across a provider call.
	inFlight *credentialRefresh
	waiters  int

	// refreshSequence orders refresh attempts, and appliedSequence records
	// the attempt whose result currently occupies creds. If two attempts ever
	// overlap, a late older result cannot replace a newer one.
	refreshSequence uint64
	appliedSequence uint64
	haveCreds       bool

	// lastRefreshErr retains the most recent provider/chain failure. A
	// still-valid cached value may be returned to callers while this remains
	// non-nil.
	lastRefreshErr error

	// CurrentTime optionally overrides the clock used for the cached Value's
	// hard Expiration. It mirrors Expiry.CurrentTime and is primarily useful
	// in boundary tests. Now is an equivalent shorter hook name.
	CurrentTime func() time.Time
	Now         func() time.Time

	// test clock aliases keep white-box tests independent of the hook name.
	now         func() time.Time
	nowFunc     func() time.Time
	timeNow     func() time.Time
	currentTime func() time.Time
}

// credentialRefresh is the shared result of one provider retrieval.
type credentialRefresh struct {
	done chan struct{}

	sequence uint64
	err      error

	// result and resultErr are finalized once, under Credentials.Lock, before
	// done is closed. Every waiter therefore observes the same outcome.
	result     Value
	resultErr  error
	panicValue any
	goexit     bool
}

// New returns a pointer to a new Credentials with the provider set.
func New(provider Provider) *Credentials {
	return &Credentials{
		provider:     provider,
		forceRefresh: true,
	}
}

// Get returns the credentials value, or error if the credentials Value failed
// to be retrieved.
//
// Will return the cached credentials Value if it has not expired. If the
// credentials Value has expired the Provider's Retrieve() will be called
// to refresh the credentials.
//
// If Credentials.Expire() was called the credentials Value will be force
// expired, and the next call to Get() will cause them to be refreshed.
//
// Deprecated: Get() exists for historical compatibility and should not be
// used. To get new credentials use the Credentials.GetWithContext function
// to ensure the proper context (i.e. HTTP client) will be used.
func (c *Credentials) Get() (Value, error) {
	return c.GetWithContext(nil)
}

// GetWithContext returns the credentials value, or error if the
// credentials Value failed to be retrieved.
//
// Will return the cached credentials Value if it has not expired. If the
// credentials Value has expired the Provider's Retrieve() will be called
// to refresh the credentials.
//
// If Credentials.Expire() was called the credentials Value will be force
// expired, and the next call to Get() will cause them to be refreshed.
func (c *Credentials) GetWithContext(cc *CredContext) (Value, error) {
	if c == nil {
		return Value{}, nil
	}
	if cc == nil {
		cc = defaultCredContext
	}

	for {
		c.Lock()
		if refresh := c.inFlight; refresh != nil {
			c.Unlock()
			return c.waitForRefresh(cc, refresh)
		}

		if !c.isExpired() {
			creds := c.creds
			c.Unlock()
			return creds, nil
		}

		c.refreshSequence++
		refresh := &credentialRefresh{
			done:     make(chan struct{}),
			sequence: c.refreshSequence,
		}
		c.inFlight = refresh
		c.Unlock()

		c.runRefresh(refresh, cc)
		return c.waitForRefresh(cc, refresh)
	}
}

// waitForRefresh waits for the shared result while honoring the caller's
// context. A result that is already available wins over context cancellation.
func (c *Credentials) waitForRefresh(cc *CredContext, refresh *credentialRefresh) (Value, error) {
	c.Lock()
	c.waiters++
	c.Unlock()
	defer func() {
		c.Lock()
		c.waiters--
		c.Unlock()
	}()

	ctx := cc.requestContext()

	select {
	case <-refresh.done:
	case <-ctx.Done():
		// Prefer a result that completed concurrently with cancellation.
		select {
		case <-refresh.done:
		default:
			return Value{}, ctx.Err()
		}
	}

	if refresh.panicValue != nil {
		panic(refresh.panicValue)
	}
	if refresh.goexit {
		// Match the provider/runtime semantics: a Goexit in the shared
		// retrieval goroutine terminates each waiting caller's goroutine.
		runtime.Goexit()
	}

	// If this caller's context was canceled but the provider failure was
	// caused by that cancellation, report the caller's context error rather
	// than serving stale credentials to a caller that has already gone away.
	if err := ctx.Err(); err != nil && refresh.err != nil {
		return Value{}, err
	}
	return refresh.result, refresh.resultErr
}

// runRefresh calls the provider exactly once for a refresh generation and
// finalizes the shared result.
func (c *Credentials) runRefresh(refresh *credentialRefresh, cc *CredContext) {
	completed := false
	defer func() {
		if r := recover(); r != nil {
			c.finishRefresh(refresh, Value{}, nil, r, false)
			// Preserve the historical behavior of panicking on the retrieving
			// goroutine's caller. Shared waiters re-panic the same value.
			panic(r)
		}
		if !completed {
			// runtime.Goexit runs defers without a recover value. Finalize the
			// waiters, then continue unwinding this goroutine.
			c.finishRefresh(refresh, Value{}, errCredentialRefreshGoexit, nil, true)
			runtime.Goexit()
		}
	}()

	value, err := c.provider.RetrieveWithCredContext(cc)
	completed = true
	c.finishRefresh(refresh, value, err, nil, false)
}

// errCredentialRefreshGoexit is delivered to direct waiters when the provider
// exits their shared retrieval goroutine with runtime.Goexit.
var errCredentialRefreshGoexit = errors.New("credential retrieval exited")

func (c *Credentials) finishRefresh(refresh *credentialRefresh, value Value, err error, panicValue any, goexit bool) {
	c.Lock()
	defer c.Unlock()

	if c.inFlight == refresh {
		c.inFlight = nil
	}
	refresh.err = err
	refresh.panicValue = panicValue
	refresh.goexit = goexit
	c.lastRefreshErr = err

	switch {
	case panicValue != nil || goexit:
		refresh.result = Value{}
		if err != nil {
			refresh.resultErr = err
		} else if panicValue != nil {
			refresh.resultErr = fmt.Errorf("credential retrieval panic: %v", panicValue)
		} else {
			refresh.resultErr = errCredentialRefreshGoexit
		}

	case err == nil:
		// A newer attempt may already have committed if attempts overlapped.
		// Never let this older result move the cache backwards.
		if refresh.sequence >= c.appliedSequence {
			c.creds = value
			c.haveCreds = true
			c.forceRefresh = false
			c.appliedSequence = refresh.sequence
		}
		refresh.result = c.creds
		refresh.resultErr = nil

	default:
		// Proactive refresh (or a call to Expire) can fail while the
		// previously cached credentials are still inside their real lifetime.
		// Keep serving them until their actual Expiration; the failed refresh
		// remains available through LastRefreshError.
		if c.hasCachedValueLocked() && !c.cachedValueTrulyExpiredLocked() {
			refresh.result = c.creds
			refresh.resultErr = nil
		} else {
			refresh.result = Value{}
			refresh.resultErr = err
		}
	}

	close(refresh.done)
}

// LastRefreshError returns the most recent retrieval error, if any. A non-nil
// value does not imply that GetWithContext is failing: a still-valid cached
// credential may be served as a fallback.
func (c *Credentials) LastRefreshError() error {
	c.Lock()
	defer c.Unlock()
	return c.lastRefreshErr
}

func (c *Credentials) hasCachedValueLocked() bool {
	if c.haveCreds {
		return true
	}
	// Allow white-box callers and older test setups that assign c.creds
	// directly to be recognized without setting the internal bool.
	return c.creds != (Value{})
}

func (c *Credentials) nowTime() time.Time {
	switch {
	case c.CurrentTime != nil:
		return c.CurrentTime()
	case c.Now != nil:
		return c.Now()
	case c.currentTime != nil:
		return c.currentTime()
	case c.nowFunc != nil:
		return c.nowFunc()
	case c.now != nil:
		return c.now()
	case c.timeNow != nil:
		return c.timeNow()
	default:
		return time.Now()
	}
}

// cachedValueTrulyExpiredLocked reports expiration against Value.Expiration,
// the hard lifetime. Provider.IsExpired may return true earlier to enter the
// refresh lead window. The boundary follows Expiry.IsExpired: the exact
// Expiration instant is still accepted and the credential is expired only
// after that instant.
func (c *Credentials) cachedValueTrulyExpiredLocked() bool {
	if !c.hasCachedValueLocked() {
		return true
	}
	expiration := c.creds.Expiration
	if expiration.IsZero() {
		return false
	}
	return expiration.Before(c.nowTime())
}

// Expire expires the credentials and forces them to be retrieved on the
// next call to Get().
//
// This will override the Provider's expired state, and force Credentials
// to call the Provider's Retrieve().
func (c *Credentials) Expire() {
	c.Lock()
	defer c.Unlock()

	c.forceRefresh = true
}

// IsExpired returns if the credentials are no longer valid, and need
// to be refreshed.
//
// If the Credentials were forced to be expired with Expire() this will
// reflect that override.
func (c *Credentials) IsExpired() bool {
	c.Lock()
	defer c.Unlock()

	return c.isExpired()
}

// isExpired helper method wrapping the definition of expired credentials.
func (c *Credentials) isExpired() bool {
	return c.forceRefresh || c.provider.IsExpired()
}
