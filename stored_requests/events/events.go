package events

import (
	"context"
	"encoding/json"

	"github.com/prebid/prebid-server/stored_requests"
)

// EventProducer will produce cache update and invalidation events on its channels
type EventProducer interface {
	Updates() <-chan map[string]json.RawMessage
	Invalidations() <-chan []string
}

// EventListener provides information about how many events a listener has processed
// and a mechanism to stop the listener goroutine
type EventListener interface {
	InvalidationCount() int
	UpdateCount() int
	Stop()
}

type eventListener struct {
	invalidationCount int
	updateCount       int
	stop              chan struct{}
}

func (e eventListener) InvalidationCount() int {
	return e.invalidationCount
}

func (e eventListener) UpdateCount() int {
	return e.updateCount
}

func (e *eventListener) Stop() {
	e.stop <- struct{}{}
}

// Listen will run a goroutine that updates/invalidates the cache when events occur
func Listen(cache stored_requests.Cache, events EventProducer) EventListener {
	listener := &eventListener{
		invalidationCount: 0,
		updateCount:       0,
		stop:              make(chan struct{}),
	}

	go func() {
		for {
			select {
			case data := <-events.Updates():
				cache.Update(context.Background(), data)
				listener.updateCount++
			case ids := <-events.Invalidations():
				cache.Invalidate(context.Background(), ids)
				listener.invalidationCount++
			case <-listener.stop:
				break
			}
		}
	}()

	return listener
}
