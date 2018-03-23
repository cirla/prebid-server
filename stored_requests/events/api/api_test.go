package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/julienschmidt/httprouter"
	"github.com/prebid/prebid-server/stored_requests/events"

	"github.com/prebid/prebid-server/config"
	"github.com/prebid/prebid-server/stored_requests/caches/in_memory"
)

func TestGoodRequests(t *testing.T) {
	cache := in_memory.NewLRUCache(&config.InMemoryCache{
		Size: 512 * 1024,
		TTL:  -1,
	})

	apiEvents, endpoint := NewEventsAPI()
	listener := events.Listen(cache, apiEvents)
	defer listener.Stop()

	id := "1"
	config := fmt.Sprintf(`{"id": "%s"}`, id)
	request, params := newRequest("POST", id, config)

	recorder := httptest.NewRecorder()
	endpoint(recorder, request, params)

	if recorder.Code != http.StatusOK {
		t.Errorf("Unexpected error from request: %s", recorder.Body.String())
	}

	for listener.UpdateCount() < 1 {
		// wait for listener goroutine to process the event
	}
	data := cache.Get(context.Background(), []string{id})
	if value, ok := data[id]; !ok || string(value) != config {
		t.Errorf("Key/Value not present in cache after update.")
	}

	request, params = newRequest("DELETE", id, "")
	recorder = httptest.NewRecorder()
	endpoint(recorder, request, params)

	if recorder.Code != http.StatusOK {
		t.Errorf("Unexpected error from request: %s", recorder.Body.String())
	}

	for listener.InvalidationCount() < 1 {
		// wait for listener goroutine to process the event
	}
	data = cache.Get(context.Background(), []string{id})
	if _, ok := data[id]; ok {
		t.Errorf("Key/Value still present in cache after invalidation.")
	}
}

func TestBadRequests(t *testing.T) {
	cache := in_memory.NewLRUCache(&config.InMemoryCache{
		Size: 512 * 1024,
		TTL:  -1,
	})

	apiEvents, endpoint := NewEventsAPI()
	listener := events.Listen(cache, apiEvents)
	defer listener.Stop()

	id := "1"
	config := "NOT JSON"
	request, params := newRequest("POST", id, config)

	recorder := httptest.NewRecorder()
	endpoint(recorder, request, params)

	if recorder.Code != http.StatusBadRequest {
		t.Errorf("Expected error from request, got OK")
	}

	request, params = newRequest("GET", id, "")
	recorder = httptest.NewRecorder()
	endpoint(recorder, request, params)

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected error from request, got OK")
	}
}

func newRequest(method string, id string, body string) (*http.Request, httprouter.Params) {
	return httptest.NewRequest(method, fmt.Sprintf("/stored_requests/%s", id), strings.NewReader(body)),
		httprouter.Params{httprouter.Param{Key: "id", Value: id}}
}
