package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"syscall"
	"time"
)

var (
	SubscriptionApi = "http://localhost:8700"
	ListenerAddr    = ":8787"
	OutputDirectory = "events"
)

type Hash struct {
	Hash string
}

type Event struct {
	IdentityChainID Hash                   `json:"identityChainID"`
	StreamSource    int32                  `json:"streamSource"`
	Value           map[string]interface{} `json:"value"`
}

func main() {
	// create output directory to store file
	createOutputFolder(OutputDirectory)

	// setup listener server
	setupListener()

	// subscribe to the live feed api
	id := subscribe()

	// delete the subscription
	defer unsubscribe(id)

	// wait
	select {}
}

func subscribe() string {
	type Subscription struct {
		Id           string `json:"id"`
		CallbackUrl  string `json:"callbackUrl"`
		CallbackType string `json:"callbackType"`
		Filters      map[string]struct {
			Filtering string `json:"filtering"`
		} `json:"filters"`
	}

	subscription := &Subscription{
		CallbackUrl:  fmt.Sprintf("http://localhost%s/callback", ListenerAddr),
		CallbackType: "HTTP",
		Filters: map[string]struct {
			Filtering string `json:"filtering"`
		}{
			"BLOCK_COMMIT":               {Filtering: ""},
			"CHAIN_REGISTRATION":         {Filtering: ""},
			"ENTRY_REGISTRATION":         {Filtering: ""},
			"ENTRY_CONTENT_REGISTRATION": {Filtering: ""},
			"NODE_MESSAGE":               {Filtering: ""},
			"PROCESS_MESSAGE":            {Filtering: ""},
		},
	}

	content, err := json.Marshal(subscription)
	if err != nil {
		log.Fatalf("failed create create subscribe request: %v", err)
	}
	url := fmt.Sprintf("%s/subscriptions", SubscriptionApi)
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(content))
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

	err = json.Unmarshal(body, subscription)
	if err != nil {
		log.Fatalf("failed to unmarshal subscribe body: %v: in %s", err, body)
	}

	log.Printf("subcribe response '%d': %s", response.StatusCode, body)
	return subscription.Id
}

func unsubscribe(id string) {
	url := fmt.Sprintf("%s/unsubscribe/%s", SubscriptionApi, id)
	request, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		log.Fatalf("failed create create unsubscribe request: %v", err)
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		log.Fatalf("failed to get unsubscribe response: %v", err)
	}

	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Fatalf("failed to get unsubscribe body: %v", err)
	}

	log.Printf("unsubcribe response '%d': %s", response.StatusCode, body)
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
	for eventType, _ := range event.Value {
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
