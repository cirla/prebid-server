package api

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/prebid/prebid-server/stored_requests/events"
)

type eventsAPI struct {
	invalidations chan []string
	updates       chan map[string]json.RawMessage
}

// NewEventsAPI creates an EventProducer that generates cache events from HTTP requests.
// The returned httprouter.Handle must be registered on both POST (update) and DELETE (invalidate)
// methods and provided an `:id` param via the URL, e.g.:
//
// apiEvents, apiEventsHandler, err := NewEventsApi()
// router.POST("/stored_requests/:id", apiEventsHandler)
// router.DELETE("/stored_requests/:id", apiEventsHandler)
// events.Listen(cache, apiEvents)
func NewEventsAPI() (events.EventProducer, httprouter.Handle, error) {
	api := &eventsAPI{
		invalidations: make(chan []string),
		updates:       make(chan map[string]json.RawMessage),
	}
	return api, httprouter.Handle(api.HandleEvent), nil
}

func (api *eventsAPI) HandleEvent(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("id")

	if r.Method == "POST" {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Missing config data.\n"))
			return
		}

		// check if JSON (TODO: validate that it is a valid request config?)
		var config json.RawMessage
		if err := json.Unmarshal(body, &config); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Invalid config data.\n"))
			return
		}

		api.updates <- map[string]json.RawMessage{id: config}

	} else if r.Method == "DELETE" {
		api.invalidations <- []string{id}
	}
}

func (api *eventsAPI) Invalidations() <-chan []string {
	return api.invalidations
}

func (api *eventsAPI) Updates() <-chan map[string]json.RawMessage {
	return api.updates
}
