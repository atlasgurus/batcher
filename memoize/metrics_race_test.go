package memoize

import (
	"sync"
	"testing"
)

// recordingCollector keeps the last TotalItems it saw. It does no locking of
// its own, as a simple collector would not.
type recordingCollector struct{ total int }

func (c *recordingCollector) Setup(function interface{}) {}
func (c *recordingCollector) Collect(m *MemoMetrics)     { c.total = m.TotalItems }

// TotalItems is written under the cache's mutex; Collect used to read it, and
// run, outside that mutex, so concurrent calls raced. Run with -race.
func TestCollectDoesNotRaceWithTheCache(t *testing.T) {
	f := Memoize(func(k int) int { return k }, WithMetrics(&recordingCollector{}), WithMaxSize(4))
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				f((g*200 + i) % 16)
			}
		}(g)
	}
	wg.Wait()
}
