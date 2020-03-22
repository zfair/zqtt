package broker

import (
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"testing"
)

const TestBrokerAddress = "tcp://127.0.0.1:9798"

// func newTestServer(
// 	tcpAderess string,
// ) (*Server, error) {
// 	config := config.NewConfig()
// 	config.TCPAddress = tcpAderess

// 	return NewServer(config)
// }

func newTestClient(
	brokerAddress string,
	password string,
	username string,
) MQTT.Client {
	opts := MQTT.NewClientOptions()
	opts.AddBroker(brokerAddress)
	opts.SetUsername(username)
	opts.SetPassword(password)

	client := MQTT.NewClient(opts)
	return client
}

func TestConnectToBroker(t *testing.T) {
	client := newTestClient(
		TestBrokerAddress,
		"test",
		"test",
	)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("err: %#v\n", token.Error())
	}
}
