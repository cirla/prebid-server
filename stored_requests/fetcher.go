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

type composedCache struct {
	caches []Cache
}

// Compose will compose caches into a single cache.
// The returned cache will attempt to Get from the caches in the order in which. they are provided,
// stopping as soon as a value is found (or when all caches have been exhausted).
// Invalidations and updates are propagated to all underlying caches.
func Compose(caches []Cache) Cache {
	return &composedCache{
		caches: caches,
	}
}

func (c *composedCache) Get(ctx context.Context, ids []string) (data map[string]json.RawMessage) {
	data = make(map[string]json.RawMessage)
	remainingIds := ids

	for _, cache := range c.caches {
		if cachedData := cache.Get(ctx, remainingIds); len(cachedData) > 0 {
			// iterate over remainingIds from end, droppings ids as they are filled
			for i := len(remainingIds) - 1; i >= 0; i-- {
				if config, ok := cachedData[remainingIds[i]]; ok {
					data[remainingIds[i]] = config
					remainingIds = append(remainingIds[:i], remainingIds[i+1:]...)
				}
			}
		}

		// return if all ids filled
		if len(remainingIds) == 0 {
			return
		}
	}
	return
}

func (c *composedCache) Invalidate(ctx context.Context, ids []string) {
	for _, cache := range c.caches {
		cache.Invalidate(ctx, ids)
	}
}

func (c *composedCache) Update(ctx context.Context, data map[string]json.RawMessage) {
	for _, cache := range c.caches {
		cache.Update(ctx, data)
	}
}

type fetcherWithCache struct {
	fetcher Fetcher
	cache   Cache
}

// WithCache returns a Fetcher which uses the given Cache before delegating to the original.
// This can be called multiple times to compose Cache layers onto the backing Fetcher, though
// it is usually more desirable to first compose caches with Compose, ensuring propagation of updates
// and invalidations through all cache layers.
func WithCache(fetcher Fetcher, cache Cache) Fetcher {
	return &fetcherWithCache{
		cache:   cache,
		fetcher: fetcher,
	}
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
