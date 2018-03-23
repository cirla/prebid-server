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
	Count() int
	Stop()
}

type eventListener struct {
	count int
	stop  chan struct{}
}

func (e eventListener) Count() int {
	return e.count
}

func (e *eventListener) Stop() {
	e.stop <- struct{}{}
}

// Listen will run a goroutine that updates/invalidates the cache when events occur
func Listen(cache stored_requests.Cache, events EventProducer) EventListener {
	listener := &eventListener{
		count: 0,
		stop:  make(chan struct{}),
	}

	go func() {
		for {
			select {
			case data := <-events.Updates():
				cache.Update(context.Background(), data)
				listener.count++
			case ids := <-events.Invalidations():
				cache.Invalidate(context.Background(), ids)
				listener.count++
			case <-listener.stop:
				break
			}
		}
	}()

	return listener
}
