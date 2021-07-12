package dagstore

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestThrottler(t *testing.T) {
	tt := NewThrottler(5)

	var cnt int32
	ch := make(chan struct{}, 16)
	fn := func(ctx context.Context) error {
		<-ch
		atomic.AddInt32(&cnt, 1)
		return nil
	}

	// spawn 10 processes; all of them will block consuming from ch.
	grp, _ := errgroup.WithContext(context.Background())
	for i := 0; i < 10; i++ {
		grp.Go(func() error {
			return tt.Do(context.Background(), fn)
		})
	}

	time.Sleep(100 * time.Millisecond)

	// counter is still 0.
	require.Zero(t, atomic.LoadInt32(&cnt))

	// allow 5 to proceed and unblock.
	for i := 0; i < 5; i++ {
		ch <- struct{}{}
	}

	time.Sleep(100 * time.Millisecond)

	// counter is 5 but not 10.
	require.EqualValues(t, 5, atomic.LoadInt32(&cnt))

	// spawn another 10.
	for i := 0; i < 10; i++ {
		grp.Go(func() error {
			return tt.Do(context.Background(), fn)
		})
	}

	// allow 10 to proceed and unblock.
	for i := 0; i < 10; i++ {
		ch <- struct{}{}
	}

	time.Sleep(100 * time.Millisecond)

	// counter is 15 but not 20.
	require.EqualValues(t, 15, atomic.LoadInt32(&cnt))

	// test with a cancelled context.
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error)
	for i := 0; i < 10; i++ {
		go func() {
			errCh <- tt.Do(ctx, fn)
		}()
	}
	time.Sleep(100 * time.Millisecond)
	cancel()

	for i := 0; i < 10; i++ {
		require.ErrorIs(t, <-errCh, context.Canceled)
	}
}
