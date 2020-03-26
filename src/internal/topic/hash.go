package topic

import (
	"github.com/spaolacci/murmur3"
)

func Sum64(b []byte) uint64 {
	return murmur3.Sum64(b)
}
