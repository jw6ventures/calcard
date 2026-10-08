// Package lru provides a least-recently-used cache bounded by the total cost of
// what it holds.
package lru

import (
	"container/list"
	"sync"
)

// Cache is a least-recently-used cache. Each entry is charged a cost, and
// whenever the entries together cost more than the budget, the least recently
// used are evicted until they do not. It is safe for concurrent use; every
// method holds the cache's lock only for its own bookkeeping, so a value is
// never computed under it.
type Cache[K, V comparable] struct {
	mu       sync.Mutex
	budget   int
	held     int
	cost     func(K, V) int
	cloneKey func(K) K
	entries  map[K]*list.Element
	recency  *list.List
}

type entry[K, V comparable] struct {
	key     K
	value   V
	charged int
}

// New returns a cache holding entries whose costs total at most budget. A nil
// cost charges every entry 1, which makes budget an entry count. cloneKey, when
// not nil, copies a key before the cache keeps it, so a key sharing memory with
// something larger -- a string read out of a request body -- does not keep that
// alive.
func New[K, V comparable](budget int, cost func(K, V) int, cloneKey func(K) K) *Cache[K, V] {
	return &Cache[K, V]{
		budget:   budget,
		cost:     cost,
		cloneKey: cloneKey,
		entries:  make(map[K]*list.Element),
		recency:  list.New(),
	}
}

// Get returns the value held under key, marking it most recently used.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.entries[key]
	if !ok {
		var zero V
		return zero, false
	}
	c.recency.MoveToFront(element)
	return element.Value.(*entry[K, V]).value, true
}

// Add holds value under key unless the key already holds one, and returns the
// value the key holds afterwards, marked most recently used.
func (c *Cache[K, V]) Add(key K, value V) V {
	return c.GetOrAdd(key, func(K) V { return value })
}

// GetOrAdd returns the value held under key, marking it most recently used.
// When there is none, it holds the one create returns for the cache's copy of
// key. create runs under the cache's lock, so it should only assemble a value,
// not compute one.
func (c *Cache[K, V]) GetOrAdd(key K, create func(key K) V) V {
	c.mu.Lock()
	defer c.mu.Unlock()
	if element, ok := c.entries[key]; ok {
		c.recency.MoveToFront(element)
		return element.Value.(*entry[K, V]).value
	}
	if c.cloneKey != nil {
		key = c.cloneKey(key)
	}
	added := &entry[K, V]{key: key, value: create(key)}
	c.entries[key] = c.recency.PushFront(added)
	c.chargeLocked(added)
	return added.value
}

// Recharge charges the entry under key what its value costs now, for a value
// whose cost changed after it was added, marks it most recently used, and
// evicts the least recently used entries until the cache is within its budget
// again. It reports false, changing nothing, when key no longer holds value.
func (c *Cache[K, V]) Recharge(key K, value V) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.entries[key]
	if !ok {
		return false
	}
	held := element.Value.(*entry[K, V])
	if held.value != value {
		return false
	}
	c.recency.MoveToFront(element)
	c.chargeLocked(held)
	return true
}

// chargeLocked records what e costs now and evicts from the least recently used
// end until the cache is within its budget. e is the most recently used, so it
// is evicted last, and only when it alone costs more than the budget.
func (c *Cache[K, V]) chargeLocked(e *entry[K, V]) {
	cost := 1
	if c.cost != nil {
		cost = c.cost(e.key, e.value)
	}
	c.held += cost - e.charged
	e.charged = cost
	for c.held > c.budget && c.recency.Len() > 0 {
		evicted := c.recency.Remove(c.recency.Back()).(*entry[K, V])
		delete(c.entries, evicted.key)
		c.held -= evicted.charged
	}
}

// Len is the number of entries held.
func (c *Cache[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.recency.Len()
}

// Held is what the entries held were charged in total.
func (c *Cache[K, V]) Held() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.held
}

// Budget is the most the entries held may cost together.
func (c *Cache[K, V]) Budget() int {
	return c.budget
}

// Range calls visit with each entry, most recently used first, until visit
// returns false. It holds the cache's lock throughout, so visit must not use
// the cache.
func (c *Cache[K, V]) Range(visit func(key K, value V) bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for element := c.recency.Front(); element != nil; element = element.Next() {
		held := element.Value.(*entry[K, V])
		if !visit(held.key, held.value) {
			return
		}
	}
}
