package zerrors

import "github.com/pkg/errors"

var (
	ErrConnExited         = errors.New("Conn Exied")
	ErrSSIDNotFound       = errors.New("SSID Not Found")
	ErrSubscriberNotFound = errors.New("Subscriber Not Found")
)
