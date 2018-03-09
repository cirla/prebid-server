package file_fetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path"
	"strings"

	"github.com/golang/glog"

	"github.com/fsnotify/fsnotify"

	"github.com/prebid/prebid-server/stored_requests"
)

// NewFileFetcher _immediately_ loads stored request data from local files.
// These are stored in memory for low-latency reads.
//
// This expects each file in the directory to be named "{config_id}.json".
// For example, when asked to fetch the request with ID == "23", it will return the data from "directory/23.json".
//
// Providing a value of true for `watch` will monitor loaded files for changes and reload as
// necessary.
func NewFileFetcher(directory string, watch bool) (stored_requests.CacheableFetcher, error) {
	fileInfos, err := ioutil.ReadDir(directory)
	if err != nil {
		return nil, err
	}

	var watcher *fsnotify.Watcher
	if watch {
		watcher, err = fsnotify.NewWatcher()
		if err != nil {
			return nil, err
		}
	}

	storedReqs := make(map[string]json.RawMessage, len(fileInfos))
	for _, fileInfo := range fileInfos {
		filePath := path.Join(directory, fileInfo.Name())
		if strings.HasSuffix(fileInfo.Name(), ".json") {
			fileData, err := ioutil.ReadFile(filePath)
			if err != nil {
				return nil, err
			}
			storedReqs[strings.TrimSuffix(fileInfo.Name(), ".json")] = json.RawMessage(fileData)
			if watch {
				if err = watcher.Add(filePath); err != nil {
					return nil, err
				}
			}
		}
	}

	fetcher := &eagerFetcher{
		stored_requests.Subscriptions{},
		storedReqs,
		watcher,
	}

	if watch {
		go func() {
			for {
				select {
				case event := <-watcher.Events:
					if event.Op&fsnotify.Write == fsnotify.Write {
						filePath := event.Name
						glog.Infof("Reloading file: %s", filePath)
						fileData, err := ioutil.ReadFile(filePath)
						if err != nil {
							glog.Errorf("Error reloading file: %v", err)
						}
						fileName := path.Base(filePath)
						id := strings.TrimSuffix(fileName, ".json")
						update := map[string]json.RawMessage{id: fileData}
						fetcher.storedReqs[id] = update[id]

						// notify subscribed Caches
						fetcher.Update(context.Background(), update)
					}
				case err := <-watcher.Errors:
					glog.Errorf("Error watching files in FileFetcher: %v", err)
				}
			}
		}()
	}

	return fetcher, nil
}

type eagerFetcher struct {
	stored_requests.Subscriptions
	storedReqs map[string]json.RawMessage
	watcher    *fsnotify.Watcher
}

func (fetcher *eagerFetcher) FetchRequests(ctx context.Context, ids []string) (map[string]json.RawMessage, []error) {
	var errors []error
	for _, id := range ids {
		if _, ok := fetcher.storedReqs[id]; !ok {
			errors = append(errors, fmt.Errorf("No config found for id: %s", id))
		}
	}

	// Even though there may be many other IDs here, the interface contract doesn't prohibit this.
	// Returning the whole slice is much cheaper than making partial copies on each call.
	return fetcher.storedReqs, errors
}
