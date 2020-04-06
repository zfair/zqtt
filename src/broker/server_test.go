package broker

import (
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
}
