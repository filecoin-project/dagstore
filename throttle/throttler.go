package throttle

import "context"

// Throttler is a component to perform throttling of concurrent requests.
type Throttler interface {
	// Do performs the supplied action under the guard of the throttler.
	//
	// The supplied context is obeyed when parking to claim a throttler spot, and is
	// passed to the action. Errors from the action are propagated to the caller,
	// as are context deadline errors.
	//
	// Do blocks until the action has executed.
	Do(context.Context, func(ctx context.Context) error) error
}

type throttler struct {
	ch chan struct{}
}

// Fixed creates a new throttler that allows the specified fixed concurrency
// at most.
func Fixed(maxConcurrency int) Throttler {
	ch := make(chan struct{}, maxConcurrency)
	for i := 0; i < maxConcurrency; i++ {
		ch <- struct{}{}
	}
	return &throttler{ch: ch}
}

func (t *throttler) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	select {
	case <-t.ch:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { t.ch <- struct{}{} }()
	return fn(ctx)
}

// Noop returns a noop throttler.
func Noop() Throttler {
	return noopThrottler{}
}

type noopThrottler struct{}

func (noopThrottler) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	return fn(ctx)
}
