package ids

import (
	"sort"
	"strings"
	"sync"
	"testing"
)

func TestNewPreservesPrefixAndSeparator(t *testing.T) {
	const prefix = "msg"

	id := New(prefix)
	if !strings.HasPrefix(id, prefix+"_") {
		t.Fatalf("New(%q) = %q, want prefix plus underscore separator", prefix, id)
	}

	const suffixWidth = 10 + 4 + 8
	if got, want := len(strings.TrimPrefix(id, prefix+"_")), suffixWidth; got != want {
		t.Fatalf("suffix width = %d, want %d", got, want)
	}
}

func TestNewGeneratesUniqueIDsInRapidBatch(t *testing.T) {
	const count = 10_000

	seen := make(map[string]struct{}, count)
	for i := 0; i < count; i++ {
		id := New("evt")
		if _, ok := seen[id]; ok {
			t.Fatalf("duplicate ID at index %d: %s", i, id)
		}
		seen[id] = struct{}{}
	}
}

func TestMinterSequentialIDsSortMonotonically(t *testing.T) {
	const count = 5_000

	minter := &Minter{}
	ids := make([]string, count)
	for i := range ids {
		ids[i] = minter.New("run")
		if i > 0 && ids[i] < ids[i-1] {
			t.Fatalf("ID at index %d sorted before previous ID: %s < %s", i, ids[i], ids[i-1])
		}
	}

	sorted := append([]string(nil), ids...)
	sort.Strings(sorted)
	for i := range ids {
		if sorted[i] != ids[i] {
			t.Fatalf("minted order differs from lexicographic order at index %d: minted=%s sorted=%s", i, ids[i], sorted[i])
		}
	}
}

func TestMinterConcurrentIDsAreUnique(t *testing.T) {
	const (
		workers   = 32
		perWorker = 250
	)

	minter := &Minter{}
	ids := make(chan string, workers*perWorker)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				ids <- minter.New("task")
			}
		}()
	}
	wg.Wait()
	close(ids)

	seen := make(map[string]struct{}, workers*perWorker)
	for id := range ids {
		if _, ok := seen[id]; ok {
			t.Fatalf("duplicate concurrent ID: %s", id)
		}
		seen[id] = struct{}{}
	}
	if got, want := len(seen), workers*perWorker; got != want {
		t.Fatalf("collected %d IDs, want %d", got, want)
	}
}

func TestEncodeFixedPadsRoundTripsSortsAndRollsOver(t *testing.T) {
	const width = 4
	max := uint64(1)<<(5*width) - 1

	cases := []struct {
		name  string
		value uint64
		want  string
	}{
		{name: "zero", value: 0, want: "0000"},
		{name: "one", value: 1, want: "0001"},
		{name: "last single digit", value: 31, want: "000Z"},
		{name: "carry", value: 32, want: "0010"},
		{name: "max", value: max, want: "ZZZZ"},
		{name: "rollover", value: max + 1, want: "0000"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := encodeFixed(tc.value, width)
			if got != tc.want {
				t.Fatalf("encodeFixed(%d, %d) = %q, want %q", tc.value, width, got, tc.want)
			}
			if len(got) != width {
				t.Fatalf("encodeFixed(%d, %d) width = %d, want %d", tc.value, width, len(got), width)
			}
			if tc.value <= max {
				if decoded := decodeFixedForTest(t, got); decoded != tc.value {
					t.Fatalf("decodeFixedForTest(%q) = %d, want %d", got, decoded, tc.value)
				}
			}
		})
	}

	orderedValues := []uint64{0, 1, 31, 32, 33, max - 1, max}
	encoded := make([]string, len(orderedValues))
	for i, value := range orderedValues {
		encoded[i] = encodeFixed(value, width)
	}
	sorted := append([]string(nil), encoded...)
	sort.Strings(sorted)
	for i := range encoded {
		if sorted[i] != encoded[i] {
			t.Fatalf("encoded values are not lexicographically sorted at index %d: encoded=%v sorted=%v", i, encoded, sorted)
		}
	}
}

func decodeFixedForTest(t *testing.T, encoded string) uint64 {
	t.Helper()

	var value uint64
	for _, r := range encoded {
		idx := strings.IndexRune(alphabet, r)
		if idx < 0 {
			t.Fatalf("encoded value %q contains non-alphabet rune %q", encoded, r)
		}
		value = (value << 5) | uint64(idx)
	}
	return value
}
