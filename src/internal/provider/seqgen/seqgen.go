package seqgen

import (
	"context"
	"io"

	"github.com/zfair/zqtt/src/config"
	"github.com/zfair/zqtt/src/internal/topic"
)

// Message Sequence Generator
type MSeqGenerator interface {
	io.Closer
	// MSeqGenerator implements a config provider.
	config.Provider
	// GenMessageSeq generate message sequence for topic
	GenMessageSeq(ctx context.Context, t *topic.Topic) (int64, error)
}
