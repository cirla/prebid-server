package events

import (
	"context"
	"encoding/json"

	"github.com/prebid/prebid-server/stored_requests"
)

// EventProducer will produce cache update and invalidation events on its channels
type EventProducer interface {
	Updates() chan map[string]json.RawMessage
	Invalidations() chan []string
}

// Listen will run a goroutine that updates/invalidates the cache when events occur
func Listen(cache stored_requests.Cache, events EventProducer) {
	go func() {
		for {
			select {
			case data := <-events.Updates():
				cache.Update(context.Background(), data)
			case ids := <-events.Invalidations():
				cache.Invalidate(context.Background(), ids)
			}
		}
	}()
}
