package system

import (
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/leeyh0216/go-bemu/internal/ports"
)

type Clock struct{}

func (Clock) Now() time.Time { return time.Now().UTC() }

type IDGenerator struct{}

func (IDGenerator) NewID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return hex.EncodeToString([]byte(time.Now().UTC().Format(time.RFC3339Nano)))
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	return hex.EncodeToString(value[:])
}

var _ ports.Clock = Clock{}
var _ ports.IDGenerator = IDGenerator{}
