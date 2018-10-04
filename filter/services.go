package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	proto "github.com/huin/mqtt"
	client "github.com/jeffallen/mqtt"
	"net"
	"os"
)

// Services
type Service interface {
	start(filters []*Filter)
	getName() string
	send(filter *Filter, message Message)
}

// =====================================
// MQTT
// =====================================
type MQTT struct {
	Broker     string
	Name       string
	Incoming   chan Message
	Connection *client.ClientConn
}

func (mqtt *MQTT) start(filters []*Filter) {
	conn, err := net.Dial("tcp", mqtt.Broker)
	if err != nil {
		fmt.Fprint(os.Stderr, "dial: ", err)
		return
	}

	mqtt.Connection = client.NewClientConn(conn)
	mqtt.Connection.Dump = false
	mqtt.Connection.ClientId = "go-filterstream"

	user := ""
	pass := ""

	// register filters
	tq := make([]proto.TopicQos, len(filters))

	for i, filter := range filters {
		tq[i].Topic = filter.In.Topic
		tq[i].Qos = proto.QosAtMostOnce
	}

	// connect
	if err := mqtt.Connection.Connect(user, pass); err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}

	// subscribe filters
	fmt.Println("Connected with client id", mqtt.Connection.ClientId)
	mqtt.Connection.Subscribe(tq)

	// DEBUG

	// =====================================
	// Incoming
	// =====================================
	for m := range mqtt.Connection.Incoming {
		buf := new(bytes.Buffer)
		m.Payload.WritePayload(buf)
		byt := buf.Bytes()

		var payload map[string]interface{}

		if err := json.Unmarshal(byt, &payload); err != nil {
			panic(err)
		}

		msg := createMessage(mqtt, m.TopicName, payload)

		mqtt.Incoming <- msg
	}
}

func (mqtt MQTT) getName() string {
	return mqtt.Name
}

func (mqtt *MQTT) send(filter *Filter, message Message) {
	var payload string
	if filter.RawOut {
		output, _ := json.Marshal(message.Payload)
		payload = string(output)
	}

	mqtt.Connection.Publish(&proto.Publish{
		Header:    proto.Header{Retain: false},
		TopicName: filter.Out.Topic,
		Payload:   proto.BytesPayload([]byte(payload)),
	})
}
