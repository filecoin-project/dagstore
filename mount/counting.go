package mount

import (
	"context"
	"sync/atomic"
)

// Counting is a mount that proxies to another mount and counts the number of
// calls made to Fetch. It is mostly used in tests.
type Counting struct {
	Mount

	n int32
}

func (c *Counting) Fetch(ctx context.Context) (Reader, error) {
	atomic.AddInt32(&c.n, 1)
	return c.Mount.Fetch(ctx)
}

func (c *Counting) Count() int {
	return int(atomic.LoadInt32(&c.n))
}
