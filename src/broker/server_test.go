package broker

import (
	"fmt"
	"testing"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

const testBrokerAddress = "tcp://127.0.0.1:9798"

func newTestClient(
	brokerAddr string,
	password string,
	username string,
) MQTT.Client {
	opts := MQTT.NewClientOptions()
	opts.AddBroker(brokerAddr)
	opts.SetUsername(username)
	opts.SetPassword(password)

	client := MQTT.NewClient(opts)
	return client
}

func TestConnectToBroker(t *testing.T) {
	client := newTestClient(
		testBrokerAddress,
		"test",
		"test",
	)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		t.Fatal(token.Error())
	}
}

func TestPublishMessage(t *testing.T) {
	client := newTestClient(
		testBrokerAddress,
		"test-publish",
		"test-publish",
	)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
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
}

func TestSubscribeMessage(t *testing.T) {

	onMessageReceived := func(client MQTT.Client, message MQTT.Message) {
		fmt.Printf("Received message on topic: %s with message id %d\nMessage: %s\n", message.Topic(), message.MessageID(), message.Payload())
	}

	client := newTestClient(
		testBrokerAddress,
		"test-subscribe",
		"test-subscribe",
	)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		t.Fatal(token.Error())
	}

	if token := client.Subscribe("go-mqtt/sample", 0, onMessageReceived); token.Wait() && token.Error() != nil {
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
