package memoize

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// returnsWithin runs fn and reports whether it returned within d. A call that
// never returns is left running; the test only needs to know it did not finish.
func returnsWithin(d time.Duration, fn func()) bool {
	done := make(chan struct{})
	go func() {
		defer func() { _ = recover() }()
		fn()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

// A caller that arrives while another computes the same key waits for that
// computation. When it ends in an error, which is not cached, the waiter must
// still be released, with the same error.
func TestWaiterIsReleasedWhenTheComputationFails(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	boom := errors.New("boom")
	f := Memoize(func(k int) (int, error) {
		close(started)
		<-release
		return 0, boom
	})

	go func() { _, _ = f(1) }()
	<-started
	var waiterErr error
	waiter := make(chan bool, 1)
	go func() {
		waiter <- returnsWithin(2*time.Second, func() { _, waiterErr = f(1) })
	}()
	time.Sleep(20 * time.Millisecond) // let the waiter find the computing entry
	close(release)

	require.True(t, <-waiter, "the waiter returns")
	assert.ErrorIs(t, waiterErr, boom, "the waiter gets the computation's error")
}

// When the computation panics, the panic reaches the caller that computed, and
// neither a concurrent waiter nor a later caller hangs: each computes again.
func TestPanicDoesNotStrandWaitersOrLaterCalls(t *testing.T) {
	var calls atomic.Int32
	started, release := make(chan struct{}), make(chan struct{})
	f := Memoize(func(k int) int {
		if calls.Add(1) == 1 {
			close(started)
			<-release
			panic("first call panics")
		}
		return 42
	})

	panicked := make(chan any, 1)
	go func() {
		defer func() { panicked <- recover() }()
		f(1)
	}()
	<-started
	var waiterGot int
	waiter := make(chan bool, 1)
	go func() {
		waiter <- returnsWithin(2*time.Second, func() { waiterGot = f(1) })
	}()
	time.Sleep(20 * time.Millisecond)
	close(release)

	assert.Equal(t, "first call panics", <-panicked, "the computing caller sees the panic")
	require.True(t, <-waiter, "a concurrent waiter returns")
	assert.Equal(t, 42, waiterGot, "the waiter computed the value itself")

	var later int
	require.True(t, returnsWithin(2*time.Second, func() { later = f(1) }), "a later call returns")
	assert.Equal(t, 42, later)
}

// Invalidating while a computation is in flight must leave the cache intact:
// when that computation finishes, it may not remove an entry it no longer
// owns. Here a newer, successful computation of the same key is cached while
// the stale one is still running; the stale one's failure must not evict it.
func TestInvalidateDuringAFailingComputation(t *testing.T) {
	var calls atomic.Int32
	started, release := make(chan struct{}), make(chan struct{})
	f, invalidate := MemoizeWithInvalidate(func(k int) (int, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-release
			return 0, errors.New("boom")
		}
		return k, nil
	})

	stale := make(chan struct{})
	go func() { _, _ = f(1); close(stale) }()
	<-started
	invalidate()

	got, err := f(1) // computes afresh and caches
	require.NoError(t, err)
	require.Equal(t, 1, got)

	close(release)
	<-stale // the stale computation finishes with an error

	got, err = f(1)
	require.NoError(t, err)
	assert.Equal(t, 1, got)
	assert.Equal(t, int32(2), calls.Load(), "the newer cached value survived the stale failure")
}

// An entry may not expire while its value is still being computed. A caller
// arriving during a computation longer than the expiration shares it rather
// than starting a second one.
func TestComputationLongerThanExpirationIsShared(t *testing.T) {
	var calls atomic.Int32
	f := Memoize(func(k int) int {
		calls.Add(1)
		time.Sleep(100 * time.Millisecond)
		return k
	}, WithExpiration(20*time.Millisecond))

	done := make(chan struct{})
	go func() { f(1); close(done) }()
	time.Sleep(50 * time.Millisecond) // past the expiration, before the value is ready
	assert.Equal(t, 1, f(1))
	<-done
	assert.Equal(t, int32(1), calls.Load(), "one computation served both callers")
}
