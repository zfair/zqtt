package broker

import (
	"fmt"
	"testing"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/zfair/zqtt/src/zqttpb"
)

const testBrokerAddress = "tcp://127.0.0.1:9798"

func newTestClient(
	brokerAddr string,
	password string,
	username string,
	clientID string,
) MQTT.Client {
	opts := MQTT.NewClientOptions()
	opts.AddBroker(brokerAddr)
	opts.SetUsername(username)
	opts.SetPassword(password)
	opts.SetClientID(clientID)

	client := MQTT.NewClient(opts)
	return client
}

func TestConnectToBroker(t *testing.T) {
	client := newTestClient(
		testBrokerAddress,
		"test",
		"test",
		"test",
	)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		t.Fatal(token.Error())
	}
}

func TestPublishMessage(t *testing.T) {
	username := "test-publish"
	password := "test-publish"
	clientID := "test-publish"
	client := newTestClient(
		testBrokerAddress,
		username,
		password,
		clientID,
	)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		t.Fatal(token.Error())
	}

	for i := 0; i < 5; i++ {
		message := zqttpb.Message{
			Username:  username,
			ClientID:  clientID,
			TopicName: "go-mqtt/sample",
			Qos:       1,
		}
		payload, err := message.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		token := client.Publish("go-mqtt/sample", 1, false, payload)
		token.Wait()
		if err := token.Error(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSubscribeMessage(t *testing.T) {

	onMessageReceived := func(client MQTT.Client, message MQTT.Message) {
		fmt.Printf("Received message on topic: %s with message id %d\nMessage: %s\n", message.Topic(), message.MessageID(), message.Payload())
	}

	client := newTestClient(
		testBrokerAddress,
		"test-subscribe",
		"test-subscribe",
		"test-subscribe",
	)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		t.Fatal(token.Error())
	}

	if token := client.Subscribe("go-mqtt/+", 0, onMessageReceived); token.Wait() && token.Error() != nil {
		t.Fatal(token.Error())
	}

	for i := 0; i < 5; i++ {
		text := fmt.Sprintf("this is msg #%d!", i)
		token := client.Publish("go-mqtt/sample", 1, false, text)
		token.Wait()
		if err := token.Error(); err != nil {
			t.Fatal(err)
		}
	}

	// select {}
}
