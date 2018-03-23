package postgres

import (
	"encoding/json"
	"time"

	"github.com/golang/glog"

	"github.com/lib/pq"
	"github.com/prebid/prebid-server/stored_requests/events"
)

type postgresEvents struct {
	invalidations chan []string
	updates       chan map[string]json.RawMessage
}

// NewPostgresEvents creates a new EventProducer listening to events on the given channel
// via Postgres LISTEN/NOTIFY
// Requires an event channel with the given name to exist on the database with payloads of the form:
// json_build_object(
//	'table',TG_TABLE_NAME,
//	'action', TG_OP,
//	'data', data
// )
// e.g.:
// {
//   "table": "stored_requests",
// 	 "action": "INSERT",
// 	 "data": {
// 	   "id": "1",
// 	   "requestData": "{\"id\": ...}"
//   }
// }
func NewPostgresEvents(connInfo string, channel string, minReconnectInterval time.Duration, maxReconnectInterval time.Duration) (events.EventProducer, error) {
	events := &postgresEvents{
		invalidations: make(chan []string),
		updates:       make(chan map[string]json.RawMessage),
	}

	reportProblem := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			glog.Errorf("Error listening to Postgres notifications: %s", err.Error())
		}
	}

	listener := pq.NewListener(connInfo, minReconnectInterval, maxReconnectInterval, reportProblem)
	if err := listener.Listen(channel); err != nil {
		return events, err
	}

	go handleNotifications(listener)

	return events, nil
}

func handleNotifications(l *pq.Listener) {
	for {
		select {
		case n := <-l.Notify:
			glog.Infof("%v", n)
			// TODO: if action == "UPDATE" -> Update({data[id]: data[requestData]})
			//       if action == "DELETE" -> Invalidate([data[id]])
			// Ignore INSERT; if it's newly added it doesn't need to be cached (nothing has queried it yet)
		case <-time.After(90 * time.Second):
			go l.Ping()
			glog.Info("Received no events for 90 seconds, checking connection")
		}
	}
}

func (e postgresEvents) Invalidations() <-chan []string {
	return e.invalidations
}

func (e postgresEvents) Updates() <-chan map[string]json.RawMessage {
	return e.updates
}
