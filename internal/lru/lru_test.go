package lru

import (
	"strings"
	"sync"
	"testing"
	"unsafe"
)

func keys[K, V comparable](c *Cache[K, V]) []K {
	var held []K
	c.Range(func(key K, _ V) bool {
		held = append(held, key)
		return true
	})
	return held
}

func TestCacheCountsEntriesWithoutACostFunction(t *testing.T) {
	c := New[string, int](2, nil, nil)
	c.Add("a", 1)
	c.Add("b", 2)
	if v, ok := c.Get("a"); !ok || v != 1 {
		t.Fatalf("Get(a) = %v, %v", v, ok)
	}
	c.Add("c", 3)
	if _, ok := c.Get("b"); ok {
		t.Error("the least recently used entry was kept past the bound")
	}
	if got := keys(c); len(got) != 2 || got[0] != "c" || got[1] != "a" {
		t.Errorf("keys most recent first = %q, want [c a]", got)
	}
	if c.Len() != 2 || c.Held() != 2 {
		t.Errorf("Len, Held = %d, %d; want 2, 2", c.Len(), c.Held())
	}
}

// Adding under a key already held keeps the value held and marks it used.
func TestCacheAddKeepsTheValueAlreadyHeld(t *testing.T) {
	c := New[string, int](2, nil, nil)
	c.Add("a", 1)
	c.Add("b", 2)
	if kept := c.Add("a", 10); kept != 1 {
		t.Errorf("Add over a held key = %d, want the held 1", kept)
	}
	c.Add("c", 3)
	if _, ok := c.Get("b"); ok {
		t.Error("re-adding a did not mark it more recently used than b")
	}
}

func TestCacheEvictsByCostWithinItsBudget(t *testing.T) {
	c := New(10, func(_ string, size int) int { return size }, nil)
	c.Add("a", 4)
	c.Add("b", 4)
	c.Add("c", 4)
	if got := keys(c); len(got) != 2 || got[0] != "c" || got[1] != "b" {
		t.Errorf("keys = %q, want [c b]", got)
	}
	if c.Held() != 8 || c.Budget() != 10 {
		t.Errorf("Held, Budget = %d, %d; want 8, 10", c.Held(), c.Budget())
	}
	// An entry costing more than the whole budget is evicted last, and so on
	// its own.
	c.Add("huge", 11)
	if c.Len() != 0 || c.Held() != 0 {
		t.Errorf("after an entry over the budget: Len, Held = %d, %d; want 0, 0", c.Len(), c.Held())
	}
}

type growing struct{ size int }

// A value whose cost changes after it was added is charged its new cost when
// recharged, the entries used least recently are evicted for it, and it is
// marked most recently used. Recharging a key that has since been evicted, or
// that holds another value, changes nothing.
func TestCacheRechargesAValueWhoseCostChanged(t *testing.T) {
	c := New(9, func(_ string, v *growing) int { return v.size }, nil)
	a, b, d := &growing{size: 2}, &growing{size: 2}, &growing{size: 2}
	c.Add("a", a)
	c.Add("b", b)
	c.Add("d", d)
	a.size = 6
	if !c.Recharge("a", a) {
		t.Fatal("Recharge(a) = false for the held value")
	}
	if got := keys(c); len(got) != 2 || got[0] != "a" || got[1] != "d" {
		t.Errorf("keys = %q, want [a d]", got)
	}
	if c.Held() != 8 {
		t.Errorf("Held = %d, want 8", c.Held())
	}
	b.size = 1
	if c.Recharge("b", b) {
		t.Error("Recharge of an evicted key = true")
	}
	if c.Recharge("d", &growing{size: 9}) {
		t.Error("Recharge with a value the key does not hold = true")
	}
	if c.Held() != 8 {
		t.Errorf("Held = %d after no-op recharges, want 8", c.Held())
	}
}

func TestCacheGetOrAddCreatesOnlyOnAMiss(t *testing.T) {
	c := New[string, *growing](4, nil, nil)
	created := 0
	create := func(string) *growing { created++; return &growing{} }
	first := c.GetOrAdd("a", create)
	if again := c.GetOrAdd("a", create); again != first || created != 1 {
		t.Errorf("second GetOrAdd = %p (created %d), want the first %p, created once", again, created, first)
	}
}

// The cache keeps a copy of a key, made by the clone function, so a key sliced
// out of a larger string does not keep that string alive; the copy is what a
// created value is handed.
func TestCacheKeepsAClonedKey(t *testing.T) {
	body := strings.Repeat("x", 64) + "Europe/Berlin"
	key := body[64:]
	c := New[string, string](4, nil, strings.Clone)
	got := c.GetOrAdd(key, func(cloned string) string { return cloned })
	if unsafe.StringData(got) == unsafe.StringData(key) {
		t.Error("create was handed the caller's key rather than the cache's copy")
	}
	c.Range(func(held, _ string) bool {
		if unsafe.StringData(held) == unsafe.StringData(key) {
			t.Error("the cache holds the caller's key")
		}
		return true
	})
	c.Add(body[60:], "")
	c.Range(func(held, _ string) bool {
		if held == body[60:] && unsafe.StringData(held) == unsafe.StringData(body[60:]) {
			t.Error("Add kept the caller's key")
		}
		return true
	})
}

func TestCacheIsSafeForConcurrentUse(t *testing.T) {
	c := New(64, func(_ int, v int) int { return v%4 + 1 }, nil)
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 2000 {
				key := (worker*7 + i) % 100
				c.GetOrAdd(key, func(k int) int { return k })
				c.Get(key + 1)
				c.Add(key+2, key)
				c.Recharge(key, key)
			}
		}()
	}
	wg.Wait()
	held := 0
	c.Range(func(_ int, v int) bool { held += v%4 + 1; return true })
	if held != c.Held() || held > c.Budget() {
		t.Errorf("entries cost %d, cache counts %d, budget %d", held, c.Held(), c.Budget())
	}
}
