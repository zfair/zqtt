package zerrors

import "github.com/pkg/errors"

var (
	ErrConnClosed         = errors.New("Connection closed")
	ErrSSIDNotFound       = errors.New("SSID not found")
	ErrSubscriberNotFound = errors.New("Subscriber not found")
)
