package gropanda

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/godepo/groat/pkg/generics"
)

func newContainer[T any](
	ctx context.Context,
	settings Settings,
	cfg config,
) *Container[T] {
	container := &Container[T]{
		forks:       &atomic.Int32{},
		ctx:         ctx,
		settings:    settings,
		injectLabel: cfg.injectLabel,
		nameSpace:   cfg.nameSpace,
	}

	return container
}

// Injector injects Redpanda settings into the provided target object.
// It also increments the fork counter and applies optional namespace prefixing.
func (c *Container[T]) Injector(t *testing.T, to T) T {
	t.Helper()

	settings := c.settings

	settings.Prefix = strconv.Itoa(int(c.forks.Add(1))) + "_"
	if c.nameSpace != "" {
		settings.Prefix = c.nameSpace + "_" + settings.Prefix
	}

	res := generics.Injector(t, &settings, to, c.injectLabel)

	return res
}
