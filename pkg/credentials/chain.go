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
	"errors"
	"strings"
	"sync"
)

// ErrNoValidProviders is returned when every provider in a Chain failed to
// provide credentials. The concrete error is usually an *AggregateError; use
// errors.As to inspect the individual provider errors.
var ErrNoValidProviders = errors.New("No valid providers found")

// ErrNoValidProvidersFoundInChain is an alias retained for callers that use
// the conventional credential-chain sentinel name.
var ErrNoValidProvidersFoundInChain = ErrNoValidProviders

// ErrNoValidProvider and ErrNoValidProvidersFound are shorter aliases.
var (
	ErrNoValidProvider       = ErrNoValidProviders
	ErrNoValidProvidersFound = ErrNoValidProviders
)

// AggregateError collects the errors returned by every provider tried by a
// Chain.
type AggregateError struct {
	// Errors is the ordered list of non-nil provider errors.
	Errors []error
}

// ChainError is an alternative name for AggregateError.
type ChainError = AggregateError

// ProvidersError is an alternative name for AggregateError.
type ProvidersError = AggregateError

// Error implements error. It preserves the historical chain error format:
// "No valid providers found [first second]".
func (e *AggregateError) Error() string {
	var b strings.Builder
	b.WriteString(ErrNoValidProviders.Error())
	b.WriteString(" [")
	for i, err := range e.Errors {
		if i > 0 {
			b.WriteByte(' ')
		}
		if err != nil {
			b.WriteString(err.Error())
		}
	}
	b.WriteByte(']')
	return b.String()
}

// Unwrap exposes every provider error to errors.Is and errors.As.
func (e *AggregateError) Unwrap() []error {
	return e.Errors
}

// Is reports whether target is the chain's no-valid-provider sentinel.
func (e *AggregateError) Is(target error) bool {
	return target == ErrNoValidProviders
}

func newAggregateError(errs []error) error {
	// Copy so later provider state changes cannot mutate a returned error.
	return &AggregateError{Errors: append([]error(nil), errs...)}
}

// A Chain will search for a provider which returns credentials
// and cache that provider until Retrieve is called again.
//
// The Chain provides a way of chaining multiple providers together
// which will pick the first available using priority order of the
// Providers in the list.
//
// If none of the Providers retrieve valid credentials Value, Retrieve()
// returns an anonymous Value together with an *AggregateError describing
// every provider failure. A provider that returns an empty Value with no
// error is skipped like an anonymous provider and contributes no error reason.
//
// If a Provider is found which returns valid credentials Value ChainProvider
// will cache that Provider for all calls to IsExpired(), until Retrieve is
// called again after IsExpired() is true.
//
//	creds := credentials.NewChainCredentials(
//	    []credentials.Provider{
//	        &credentials.EnvAWSS3{},
//	        &credentials.EnvMinio{},
//	    })
//
//	// Usage of ChainCredentials.
//	mc, err := minio.NewWithCredentials(endpoint, creds, secure, "us-east-1")
//	if err != nil {
//	     log.Fatalln(err)
//	}
type Chain struct {
	Providers []Provider

	mu   sync.Mutex
	curr Provider
}

// NewChainCredentials returns a pointer to a new Credentials object
// wrapping a chain of providers.
func NewChainCredentials(providers []Provider) *Credentials {
	return New(&Chain{
		Providers: append([]Provider{}, providers...),
	})
}

// RetrieveWithCredContext is like Retrieve with CredContext.
func (c *Chain) RetrieveWithCredContext(cc *CredContext) (Value, error) {
	return c.retrieve(cc, false)
}

// Retrieve returns the credentials value. If no provider yields credentials,
// it returns an anonymous Value and an AggregateError containing each
// provider's error.
//
// If a provider is found with credentials, it will be cached and any calls
// to IsExpired() will return the expired state of the cached provider.
//
// Deprecated: Retrieve exists for historical compatibility. Use
// RetrieveWithCredContext so request-scoped configuration is propagated.
func (c *Chain) Retrieve() (Value, error) {
	return c.retrieve(nil, true)
}

func (c *Chain) retrieve(cc *CredContext, useDeprecatedRetrieve bool) (Value, error) {
	var providerErrs []error

	for _, p := range c.Providers {
		var (
			creds Value
			err   error
		)
		if useDeprecatedRetrieve {
			creds, err = p.Retrieve()
		} else {
			creds, err = p.RetrieveWithCredContext(cc)
		}
		if err != nil {
			// Keep the reason and try the next provider. Returning early here
			// was the reason only one swallowed provider failure survived.
			providerErrs = append(providerErrs, err)
			continue
		}

		// Always prioritize non-anonymous providers, if any.
		if creds.AccessKeyID == "" && creds.SecretAccessKey == "" {
			continue
		}

		c.mu.Lock()
		c.curr = p
		c.mu.Unlock()
		return creds, nil
	}

	// Every provider either failed or returned no usable credentials. Return
	// anonymous for compatibility with callers that ignore the error, but
	// also return an identifiable aggregate carrying every provider reason.
	return Value{SignerType: SignatureAnonymous}, newAggregateError(providerErrs)
}

// IsExpired will returned the expired state of the currently cached provider
// if there is one. If there is no current provider, true will be returned.
func (c *Chain) IsExpired() bool {
	c.mu.Lock()
	curr := c.curr
	c.mu.Unlock()

	if curr != nil {
		return curr.IsExpired()
	}

	return true
}
