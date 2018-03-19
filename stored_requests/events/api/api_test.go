package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

	apiEvents, endpoint, err := NewEventsAPI()
	if err != nil {
		t.Fatalf("Error creating endpoint: %v", err)
	}

	events.Listen(cache, apiEvents)

	recorder := httptest.NewRecorder()

	id := "1"
	config := fmt.Sprintf(`{"id": "%s"}`, id)
	request, params := newRequest("POST", id, config)
	endpoint(recorder, request, params)

	if recorder.Code != http.StatusOK {
		t.Errorf("Unexpected error from request: %s", recorder.Body.String())
	}

	// TODO: find a better way to wait for channel to empty
	time.Sleep(100 * time.Millisecond)
	data := cache.Get(context.Background(), []string{id})
	if value, ok := data[id]; !ok || string(value) != config {
		t.Errorf("Key/Value not present in cache after update.")
	}
}

func newRequest(method string, id string, body string) (*http.Request, httprouter.Params) {
	return httptest.NewRequest(method, fmt.Sprintf("/stored_requests/%s", id), strings.NewReader(body)),
		httprouter.Params{httprouter.Param{Key: "id", Value: id}}
}
