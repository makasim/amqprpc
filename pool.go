package amqprpc

import (
	"time"

	"github.com/patrickmn/go-cache"
)

type pool struct {
	c *cache.Cache
}

func (p *pool) set(r request) {
	ttl := cache.DefaultExpiration

	deadline, ok := r.Ctx.Deadline()
	if ok {
		ttl = deadline.Sub(time.Now())
	}

	p.c.Set(r.CorrID, r, ttl)
}

func (p *pool) get(corrID string) (request, bool) {
	i, ok := p.c.Get(corrID)
	if !ok {
		return request{}, false
	}

	r, ok := i.(request)
	if !ok {
		return request{}, false
	}

	return r, true
}
