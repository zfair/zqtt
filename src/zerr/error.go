// Package zerr contains all project-wide errors.
package zerr

import "github.com/pkg/errors"

var (
	ErrNotConnectd          = errors.New("Not Connected")
	ErrConnClosed           = errors.New("Connection closed")
	ErrSSIDNotFound         = errors.New("SSID not found")
	ErrSubscriberNotFound   = errors.New("Subscriber not found")
	ErrNoMessageIDAvailable = errors.New("No message id available")
)
