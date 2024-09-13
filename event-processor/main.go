package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type DeviceInfo struct {
	DevEUI            string `json:"dev_eui"`
	DeviceProfileName string `json:"device_profile_name"`
}

type KafkaMessage struct {
	Event      string     `json:"event"`
	DeviceInfo DeviceInfo `json:"device_info"`
}

func main() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{"localhost:9092", "localhost:9093", "localhost:9094", "localhost:9095"},
		Topic:          "iot-stream",
		GroupID:        "event-processor-group",
		CommitInterval: 1 * time.Second,
	})

	fmt.Println("Event processor started. Waiting for messages...")

	for {
		msg, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Fatal("Error reading message: ", err)
		}

		var kafkaMsg KafkaMessage
		err = json.Unmarshal(msg.Value, &kafkaMsg)
		if err != nil {
			log.Printf("Error decoding message: %v", err)
			continue
		}

		fmt.Printf("Event: %s, DevEUI: %s, Device Profile Name: %s\n",
			kafkaMsg.Event, kafkaMsg.DeviceInfo.DevEUI, kafkaMsg.DeviceInfo.DeviceProfileName)

		fmt.Println("Event processed and removed.")
	}
}
