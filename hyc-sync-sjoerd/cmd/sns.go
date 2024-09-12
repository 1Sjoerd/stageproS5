/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"encoding/base64"
	"encoding/json"

	"fmt"
	"log"

	c "bitbucket.org/tsg-eos/hyrule/config"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/chirpstack/chirpstack/api/go/v4/integration"
	"github.com/spf13/cobra"
)

// snsCmd represents the sns command
var snsCmd = &cobra.Command{
	Use:   "sns",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		h, err := newHandler(
			// set true when using JSON encoding
			true,
			// SQS queue url
			"https://sqs.eu-west-1.amazonaws.com/992943329027/hyc-iot-toon",
		)
		if err != nil {
			panic(err)
		}

		panic(h.receive(context.Background()))
	},
}

type handler struct {
	json bool

	sqs      *sqs.Client
	queueURL string
}
type MessageAttribute struct {
	Type  string `json:"Type"`
	Value string `json:"Value"`
}
type Message struct {
	Type              string                      `json:"Type"`
	MessageID         string                      `json:"MessageId"`
	Message           string                      `json:"Message"`
	MessageAttributes map[string]MessageAttribute `json:"MessageAttributes"`
}

func (h *handler) receive(ctx context.Context) error {
	emitter, err := goka.NewEmitter(c.Brokers, IoTStream, new(codec.String))
	if err != nil {
		return err
	}

	for {
		result, err := h.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			AttributeNames: []types.QueueAttributeName{
				types.QueueAttributeNameAll,
			},
			QueueUrl:            &h.queueURL,
			MaxNumberOfMessages: 10,
		})
		if err != nil {
			return err
		}

		for _, msg := range result.Messages {
			_, err := h.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      &h.queueURL,
				ReceiptHandle: msg.ReceiptHandle,
			})
			if err != nil {
				log.Printf("delete message error: %s", err)
			}

			var message Message

			if err := json.Unmarshal([]byte(*msg.Body), &message); err != nil {
				log.Printf("unmarshal message error: %s", err)
				continue
			}

			eventAttributes, ok := message.MessageAttributes["event"]
			if !ok || eventAttributes.Value == "" {
				log.Printf("event attribute is missing")
				continue
			}

			var deveui string
			var data []byte

			switch eventAttributes.Value {
			case "up":
				deveui, data, err = h.up(message.Message)
			case "join":
				deveui, data, err = h.join(message.Message)
			case "status":
				deveui, data, err = h.status(message.Message)
			default:
				log.Printf("handler for event %s is not implemented", eventAttributes.Value)
				err = nil
				continue
			}

			if err != nil {
				log.Printf("handling event '%s' returned error: %s", eventAttributes.Value, err)
			} else {
				log.Printf("emitting event '%s' for device %s", eventAttributes.Value, deveui)
				// prefix data with event type and seperate by ,
				emitter.Emit(deveui, string(data))
			}
		}
	}

}

func (h *handler) up(body string) (string, []byte, error) {
	var up integration.UplinkEvent
	if err := h.unmarshal(body, &up); err != nil {
		return "", nil, err
	}

	data, err := h.marshal(up.ProtoReflect(), "up")
	if err != nil {
		return "", nil, err
	}

	return up.GetDeviceInfo().GetDevEui(), data, nil
}

func (h *handler) join(body string) (string, []byte, error) {
	var join integration.JoinEvent
	if err := h.unmarshal(body, &join); err != nil {
		return "", nil, err

	}

	data, err := h.marshal(join.ProtoReflect(), "join")
	if err != nil {
		return "", nil, err
	}

	return join.DeviceInfo.GetDevEui(), data, nil
}

func (h *handler) status(body string) (string, []byte, error) {
	var status integration.StatusEvent
	if err := h.unmarshal(body, &status); err != nil {
		return "", nil, err
	}

	data, err := h.marshal(status.ProtoReflect(), "status")
	if err != nil {
		return "", nil, err
	}

	return status.GetDeviceInfo().GetDevEui(), data, nil
}

func (h *handler) unmarshal(body string, v proto.Message) error {
	if h.json {
		return protojson.UnmarshalOptions{
			DiscardUnknown: true,
			AllowPartial:   true,
		}.Unmarshal([]byte(body), v)
	}

	b, err := base64.StdEncoding.DecodeString(body)
	if err != nil {
		return err
	}

	return proto.Unmarshal(b, v)
}

func (h *handler) marshal(v protoreflect.Message, event string) ([]byte, error) {
	if h.json {
		b, err := protojson.MarshalOptions{
			UseProtoNames: true,
			AllowPartial:  true,
		}.Marshal(v.Interface())
		if err != nil {
			return nil, err
		}

		b = append(b[:1], append([]byte("\"event\":\""+event+"\","), b[1:]...)...)

		return b, nil
	}
	b, err := proto.Marshal(v.Interface())
	if err != nil {
		return nil, err
	}

	return b, nil
}

func newHandler(json bool, queueURL string) (*handler, error) {

	sdkConfig, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		fmt.Println("Couldn't load default configuration. Have you set up your AWS account?")
		fmt.Println(err)
		return nil, err
	}
	sqsClient := sqs.NewFromConfig(sdkConfig)

	if err != nil {
		return nil, err
	}

	return &handler{
		json:     json,
		sqs:      sqsClient,
		queueURL: queueURL,
	}, nil
}

func init() {
	rootCmd.AddCommand(snsCmd)
}
