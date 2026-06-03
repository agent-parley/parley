package ids

import (
	"crypto/rand"
	"fmt"
	"strings"
	"sync"
	"time"
)

const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// Minter creates prefixed, lexicographically sortable IDs.
type Minter struct {
	mu      sync.Mutex
	lastMS  int64
	counter uint64
}

var defaultMinter = &Minter{}

// New returns an ID with the form prefix_sortablesuffix.
func New(prefix string) string {
	return defaultMinter.New(prefix)
}

// New returns an ID with the form prefix_sortablesuffix.
func (m *Minter) New(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = "id"
	}

	now := time.Now().UnixMilli()
	m.mu.Lock()
	if now == m.lastMS {
		m.counter++
	} else {
		m.lastMS = now
		m.counter = 0
	}
	counter := m.counter
	m.mu.Unlock()

	var entropy [5]byte
	_, _ = rand.Read(entropy[:])
	entropyValue := uint64(0)
	for _, b := range entropy {
		entropyValue = (entropyValue << 8) | uint64(b)
	}

	return fmt.Sprintf("%s_%s%s%s", prefix, encodeFixed(uint64(now), 10), encodeFixed(counter, 4), encodeFixed(entropyValue, 8))
}

func encodeFixed(v uint64, width int) string {
	buf := make([]byte, width)
	for i := width - 1; i >= 0; i-- {
		buf[i] = alphabet[v&31]
		v >>= 5
	}
	return string(buf)
}
