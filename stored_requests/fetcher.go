package stored_requests

import (
	"context"
	"encoding/json"
)

// Fetcher knows how to fetch Stored Request data by id.
//
// Implementations must be safe for concurrent access by multiple goroutines.
// Callers are expected to share a single instance as much as possible.
type Fetcher interface {
	// FetchRequests fetches the stored requests for the given IDs.
	// The returned map will have keys for every ID in the argument list, unless errors exist.
	//
	// The returned objects can only be read from. They may not be written to.
	FetchRequests(ctx context.Context, ids []string) (map[string]json.RawMessage, []error)
}

// Cache is an intermediate layer which can be used to create more complex Fetchers by composition.
// Implementations must be safe for concurrent access by multiple goroutines.
// To add a Cache layer in front of a Fetcher, see WithCache()
type Cache interface {
	// Get works much like Fetcher.FetchRequests, with a few exceptions:
	//
	// 1. Any errors should be logged by the implementation, rather than returned.
	// 2. The returned map _may_ be written to.
	// 3. The returned map must _not_ contain keys unless they were present in the argument ID list.
	// 4. Callers _should not_ assume that the returned map contains a key for every id.
	//    The returned map will miss entries for keys which don't exist in the cache.
	Get(ctx context.Context, ids []string) map[string]json.RawMessage

	// Invalidate will ensure that all values associated with the given IDs
	// are no longer returned by the cache until new values are saved via Update
	Invalidate(ctx context.Context, ids []string)

	// Update will update the given values in the cache
	Update(ctx context.Context, data map[string]json.RawMessage)
}

// CacheableFetcher is a type of Fetcher that can be composed with one or more
// Caches via WithCache and provide updates and validations to those Caches
// when necessary
type CacheableFetcher interface {
	Fetcher

	// Subscribe notifies the Fetcher that a Cache is interested in Updates and Invalidations
	// that may arise due to underlying changes to data
	Subscribe(cache Cache)
}

// CachedFetcher is the result of composing a CacheableFetcher with a Cache via WithCache
// It appears as both a Cache so that it may Subscribe to the underlying Fetcher and as
// a CacheableFetcher so that it may be composed with another Cache on top of it
type CachedFetcher interface {
	CacheableFetcher
	Cache
}

// Subscriptions encapsulates forwarding updates and invalidations to subscribed Caches
// It provides a convenient way to implement the CacheableFetcher interface by adding
// it inside an implementation struct
type Subscriptions struct {
	subs []Cache
}

// Subscribe implementation (CacheableFetcher) for Subscriptions
func (s *Subscriptions) Subscribe(cache Cache) {
	s.subs = append(s.subs, cache)
}

// Invalidate convenience method to propagate invalidation to all subscribed Caches
func (s *Subscriptions) Invalidate(ctx context.Context, ids []string) {
	for _, c := range s.subs {
		c.Invalidate(ctx, ids)
	}
}

// Update convenience method to propagate invalidation to all subscribed Caches
func (s *Subscriptions) Update(ctx context.Context, data map[string]json.RawMessage) {
	for _, c := range s.subs {
		c.Update(ctx, data)
	}
}

type fetcherWithCache struct {
	Subscriptions
	cache   Cache
	fetcher CacheableFetcher
}

// WithCache returns a Fetcher which uses the given Cache before delegating to the original.
// This can be called multiple times to compose Cache layers onto the backing Fetcher.
func WithCache(fetcher CacheableFetcher, cache Cache) CachedFetcher {
	f := &fetcherWithCache{
		cache:   cache,
		fetcher: fetcher,
	}

	// subscribe the underlying Cache to this Fetcher's updates, including
	// those propagated from the underlying Fetcher
	f.Subscribe(cache)

	// subscribe this Fetcher to the underlying Fetcher's updates
	fetcher.Subscribe(f)

	return f
}

func (f *fetcherWithCache) FetchRequests(ctx context.Context, ids []string) (data map[string]json.RawMessage, errs []error) {
	data = f.cache.Get(ctx, ids)

	missingIds := make([]string, 0, len(ids)-len(data))
	for _, id := range ids {
		if _, ok := data[id]; !ok {
			missingIds = append(missingIds, id)
		}
	}

	missingData, errs := f.fetcher.FetchRequests(ctx, missingIds)
	f.cache.Update(ctx, data)
	for key, value := range missingData {
		data[key] = value
	}

	return
}

// Get implementation to complete the implementation of CachedFetcher.
// Forwards to underlying Cache.
func (f *fetcherWithCache) Get(ctx context.Context, ids []string) map[string]json.RawMessage {
	return f.cache.Get(ctx, ids)
}
