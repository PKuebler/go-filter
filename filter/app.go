package main

import (
	"fmt"
	"strings"
)

// Config
type Config struct {
	Filters  []*Filter
	Services map[string]Service
}

// Message Handler
type Message struct {
	Service Service
	Topic   string
	_Parsed []string
	Payload map[string]interface{}
	Filter  Filter
}

func createMessage(service Service, topic string, payload map[string]interface{}) Message {
	return Message{
		Service: service,
		Topic:   topic,
		_Parsed: strings.Split(topic, "/"),
		Payload: payload,
	}
}

func main() {
	filterChannel := make(chan Message)

	// condition
	rule := Condition{
		field: "temperature",
		lte:   float64(50),
	}

	// define filters
	filterA := Filter{
		Name: "Filter A",
		In: FilterConnection{
			Topic:   "iot/+/temperature",
			Service: "mqtt1",
		},
		Out: FilterConnection{
			Topic:   "iot/lalala/alert",
			Service: "mqtt1",
		},
		Rules:  rule,
		RawOut: true,
	}

	// define mqtt
	services := make(map[string]Service)

	services["mqtt1"] = &MQTT{
		Name:     "mqtt1",
		Broker:   "localhost:1883",
		Incoming: filterChannel,
	}

	// define config
	cfg := Config{
		Filters:  []*Filter{&filterA},
		Services: services,
	}

	// parse topics
	for _, filter := range cfg.Filters {
		filter.parse()
	}

	// start services
	for _, service := range cfg.Services {
		go service.start(cfg.Filters)

		for m := range filterChannel {
			// filter msg
			for _, filter := range cfg.Filters {
				if len(filter.In.Service) > 0 && !filter.matchService(m) {
					continue
				}
				if !filter.matchTopic(m) {
					continue
				}
				if !filter.matchPaylod(m) {
					continue
				}

				fmt.Println("True")

				// send
				if len(filter.Out.Service) > 0 {
					if outService, ok := cfg.Services[filter.Out.Service]; ok {
						outService.send(filter, m)
					}
				}
			}
		}
	}
}
