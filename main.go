package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"syscall"
	"time"
)

var (
	SubscriptionApi = "http://127.0.0.1:8700/live/feed/v0.1"
	ListenerAddr    = ":8787"
	OutputDirectory = "events"
	ID              = -1
)

type Event struct {
	IdentityChainID string                 `json:"identityChainID"`
	StreamSource    int32                  `json:"streamSource"`
	Event           map[string]interface{} `json:"event"`
}

type Subscription struct {
	ID           string `json:"id"`
	CallbackUrl  string `json:"callbackUrl"`
	CallbackType string `json:"callbackType"`
	Filters      map[string]struct {
		Filtering string `json:"filtering"`
	} `json:"filters"`
}

func main() {
	// create output directory to store file
	createOutputFolder(OutputDirectory)

	// setup listener server
	setupListener()

	// subscribe to the live feed api
	subscribe()

	// delete the subscription
	defer unsubscribe()

	// wait
	select {}
}

func subscribe() {
	subscription := &Subscription{
		ID:           strconv.Itoa(ID),
		CallbackUrl:  fmt.Sprintf("http://172.16.255.32%s/callback", ListenerAddr),
		CallbackType: "HTTP",
		Filters: map[string]struct {
			Filtering string `json:"filtering"`
		}{
			"DIRECTORY_BLOCK_COMMIT": {Filtering: ""},
			"CHAIN_COMMIT":           {Filtering: ""},
			"ENTRY_COMMIT":           {Filtering: ""},
			"ENTRY_REVEAL":           {Filtering: ""},
			"STATE_CHANGE":           {Filtering: ""},
			"NODE_MESSAGE":           {Filtering: ""},
			"PROCESS_MESSAGE":        {Filtering: ""},
		},
	}

	// update existing subscription if ID is provided
	if ID >= 0 {
		url := fmt.Sprintf("%s/subscriptions/%d", SubscriptionApi, ID)

		response, statusCode, err := requestSubscription(url, http.MethodPut, subscription)

		if err != nil || statusCode != http.StatusOK {
			log.Printf("failed to update subscription '%d' returns: %s, error: %v", statusCode, response, err)
		}

		// updating existing subscription successful
		if statusCode == http.StatusOK {
			log.Printf("update subscribe response: %s", response)
			return
		}
	}

	// create new subscription
	url := fmt.Sprintf("%s/subscriptions", SubscriptionApi)
	response, statusCode, err := requestSubscription(url, http.MethodPost, subscription)

	if err != nil {
		log.Printf("failed to create subscription '%d' returns: %s, error: %v", statusCode, response, err)
		return
	}

	if statusCode != http.StatusCreated {
		log.Printf("failed to create subscription '%d' returns: %s", statusCode, response)
		return
	}

	log.Printf("subscribe response: %s", response)
}

func unsubscribe() {
	// delete the subscription
	url := fmt.Sprintf("%s/subscriptions/%d", SubscriptionApi, ID)
	response, statusCode, err := request(url, http.MethodDelete, nil)

	if err != nil {
		log.Printf("failed to delete subscribtion '%d' returns: %s, error: %v", statusCode, response, err)
		return
	}

	log.Printf("unsubcribe response '%d': %s", statusCode, response)
}

func requestSubscription(url string, method string, subscription *Subscription) (string, int, error) {
	// convert the subscription to json
	content, err := json.Marshal(subscription)
	if err != nil {
		log.Fatalf("failed create create subscribe request: %v", err)
	}

	// execute the post
	response, statusCode, err := request(url, method, bytes.NewBuffer(content))
	if err != nil {
		return response, statusCode, err
	}

	// update the provided subscription
	err = json.Unmarshal([]byte(response), subscription)
	if err != nil {
		log.Fatalf("failed to unmarshal subscription response [url=%s, code=%d, error='%v', response='%s']", url, statusCode, err, response)
	}

	return response, statusCode, err
}

func request(url string, method string, content io.Reader) (string, int, error) {
	request, err := http.NewRequest(method, url, content)
	if err != nil {
		log.Fatalf("failed create create subscribe request: %v", err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		log.Fatalf("failed to get subscribe response: %v", err)
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Fatalf("failed to get subscribe body: %v", err)
	}

	return string(body), response.StatusCode, nil
}

func setupListener() {
	http.HandleFunc("/callback", func(w http.ResponseWriter, r *http.Request) {
		// handle callback event
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("failed to recieve callback: %v", err)
			return
		}

		// log incoming event
		log.Printf("< %v", string(body))

		// handle event
		handleEvent(body)
	})

	go func() {
		log.Fatalf("failed to start server: %v", http.ListenAndServe(ListenerAddr, nil))
	}()
	time.Sleep(1 * time.Millisecond)
}

func handleEvent(body []byte) {
	var event Event
	err := json.Unmarshal(body, &event)
	if err != nil {
		log.Printf("failed to marshal event: %v: %s", err, body)
		return
	}

	// write the event to disk
	for eventType := range event.Event {
		writeFile(eventType, body)
	}
}

func writeFile(eventType string, event []byte) {
	oldMask := syscall.Umask(0)
	defer syscall.Umask(oldMask)

	// write the whole body at once
	log.Printf("writing event to file: %s", eventType)
	data := formatJson(event)

	err := ioutil.WriteFile(fmt.Sprintf("%s/%s.json", OutputDirectory, eventType), data, os.ModePerm)
	if err != nil {
		log.Printf("failed to write file: %v", err)
	}
}

func formatJson(src []byte) []byte {
	var out bytes.Buffer
	err := json.Indent(&out, src, "", "\t")
	if err != nil {
		return src
	}
	return out.Bytes()
}

func createOutputFolder(folderPath string) {
	oldMask := syscall.Umask(0)
	defer syscall.Umask(oldMask)

	if err := os.MkdirAll(folderPath, os.ModePerm); err != nil {
		log.Printf("failed to create event directory '%s': %v", folderPath, err)
	}
}
