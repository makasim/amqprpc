package amqprpc

import (
	"sync"
)

type pool struct {
	mux   sync.Mutex
	calls map[string]*Call
}

func newPool() *pool {
	return &pool{
		calls: make(map[string]*Call),
	}
}

func (p *pool) set(call *Call) {
	p.mux.Lock()
	defer p.mux.Unlock()

	p.calls[call.message.Publishing.CorrelationId] = call
}

func (p *pool) fetch(corrID string) (*Call, bool) {
	p.mux.Lock()
	defer p.mux.Unlock()

	c, ok := p.calls[corrID]
	if !ok {
		return nil, false
	}

	delete(p.calls, corrID)

	return c, true
}

func (p *pool) delete(corrID string) {
	p.mux.Lock()
	defer p.mux.Unlock()

	delete(p.calls, corrID)
}

func (p *pool) count() int {
	p.mux.Lock()
	defer p.mux.Unlock()

	return len(p.calls)
}
