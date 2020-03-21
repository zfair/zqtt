package zerrors

import "github.com/pkg/errors"

var (
	ErrSSIDNotFound       = errors.New("SSID Not Found")
	ErrSubscriberNotFound = errors.New("Subscriber Not Found")
)
