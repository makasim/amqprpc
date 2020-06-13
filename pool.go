package amqprpc

import (
	"sync"
)

type pool struct {
	lock  sync.Mutex
	calls map[string]*Call
}

func newPool() *pool {
	return &pool{
		calls: make(map[string]*Call),
	}
}

func (p *pool) set(c *Call) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.calls[c.Publishing.Message.CorrelationId] = c
}

func (p *pool) fetch(corrID string) (*Call, bool) {
	p.lock.Lock()
	defer p.lock.Unlock()

	c, ok := p.calls[corrID]
	if !ok {
		return nil, false
	}

	delete(p.calls, corrID)

	return c, true
}

func (p *pool) delete(corrID string) {
	p.lock.Lock()
	defer p.lock.Unlock()

	delete(p.calls, corrID)
}

func (p *pool) count() int {
	p.lock.Lock()
	defer p.lock.Unlock()

	return len(p.calls)
}
