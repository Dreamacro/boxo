package cache

// Modified by https://github.com/die-net/lrucache

import (
	"container/list"
	"sync"
	"time"
)

// Option is part of Functional Options Pattern
type Option[K comparable, V any] func(*LruCache[K, V])

// EvictCallback is used to get a callback when a cache entry is evicted
type EvictCallback[K comparable, V any] func(key K, value V)

// WithEvict set the evict callback
func WithEvict[K comparable, V any](cb EvictCallback[K, V]) Option[K, V] {
	return func(l *LruCache[K, V]) {
		l.onEvict = cb
	}
}

// WithUpdateAgeOnGet update expires when Get element
func WithUpdateAgeOnGet[K comparable, V any]() Option[K, V] {
	return func(l *LruCache[K, V]) {
		l.updateAgeOnGet = true
	}
}

// WithAge defined element max age (second)
func WithAge[K comparable, V any](maxAge int64) Option[K, V] {
	return func(l *LruCache[K, V]) {
		l.maxAge = maxAge
	}
}

// WithSize defined max length of LruCache
func WithSize[K comparable, V any](maxSize int) Option[K, V] {
	return func(l *LruCache[K, V]) {
		l.maxSize = maxSize
	}
}

// WithStale decide whether Stale return is enabled.
// If this feature is enabled, element will not get Evicted according to `WithAge`.
func WithStale[K comparable, V any](stale bool) Option[K, V] {
	return func(l *LruCache[K, V]) {
		l.staleReturn = stale
	}
}

// LruCache is a thread-safe, in-memory lru-cache that evicts the
// least recently used entries from memory when (if set) the entries are
// older than maxAge (in seconds).  Use the New constructor to create one.
type LruCache[K comparable, V any] struct {
	maxAge         int64
	maxSize        int
	mu             sync.Mutex
	cache          map[K]*list.Element
	lru            *list.List // Front is least-recent
	updateAgeOnGet bool
	staleReturn    bool
	onEvict        EvictCallback[K, V]
}

// New creates an LruCache
func New[K comparable, V any](options ...Option[K, V]) *LruCache[K, V] {
	lc := &LruCache[K, V]{
		lru:   list.New(),
		cache: make(map[K]*list.Element),
	}

	for _, option := range options {
		option(lc)
	}

	return lc
}

// Get returns the any representation of a cached response and a bool
// set to true if the key was found.
func (c *LruCache[K, V]) Get(key K) (V, bool) {
	entry := c.get(key)
	if entry == nil {
		return *new(V), false
	}
	value := entry.value

	return value, true
}

// GetWithExpire returns the any representation of a cached response,
// a time.Time Give expected expires,
// and a bool set to true if the key was found.
// This method will NOT check the maxAge of element and will NOT update the expires.
func (c *LruCache[K, V]) GetWithExpire(key K) (V, time.Time, bool) {
	entry := c.get(key)
	if entry == nil {
		return *new(V), time.Time{}, false
	}

	return entry.value, time.Unix(entry.expires, 0), true
}

// Exist returns if key exist in cache but not put item to the head of linked list
func (c *LruCache[K, V]) Exist(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	elm, ok := c.cache[key]
	if !ok {
		return false
	}

	if c.maxAge > 0 && elm.Value.(*entry[K, V]).expires <= time.Now().Unix() {
		return false
	}

	return true
}

// Len returns the number of items in the cache.
func (c *LruCache[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.lru.Len()
}

// Set stores the any representation of a response for a given key.
func (c *LruCache[K, V]) Set(key K, value V) {
	expires := int64(0)
	if c.maxAge > 0 {
		expires = time.Now().Unix() + c.maxAge
	}
	c.SetWithExpire(key, value, time.Unix(expires, 0))
}

// SetWithExpire stores the any representation of a response for a given key and given expires.
// The expires time will round to second.
func (c *LruCache[K, V]) SetWithExpire(key K, value V, expires time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if le, ok := c.cache[key]; ok {
		c.lru.MoveToBack(le)
		e := le.Value.(*entry[K, V])
		e.value = value
		e.expires = expires.Unix()
	} else {
		e := &entry[K, V]{key: key, value: value, expires: expires.Unix()}
		c.cache[key] = c.lru.PushBack(e)

		if c.maxSize > 0 {
			if len := c.lru.Len(); len > c.maxSize {
				c.deleteElement(c.lru.Front())
			}
		}
	}

	c.maybeDeleteOldest()
}

// CloneTo clone and overwrite elements to another LruCache
func (c *LruCache[K, V]) CloneTo(n *LruCache[K, V]) {
	c.mu.Lock()
	defer c.mu.Unlock()

	n.mu.Lock()
	defer n.mu.Unlock()

	n.lru = list.New()
	n.cache = make(map[K]*list.Element)

	for e := c.lru.Front(); e != nil; e = e.Next() {
		elm := e.Value.(*entry[K, V])
		n.cache[elm.key] = n.lru.PushBack(elm)
	}
}

func (c *LruCache[K, V]) get(key K) *entry[K, V] {
	c.mu.Lock()
	defer c.mu.Unlock()

	le, ok := c.cache[key]
	if !ok {
		return nil
	}

	if !c.staleReturn && c.maxAge > 0 && le.Value.(*entry[K, V]).expires <= time.Now().Unix() {
		c.deleteElement(le)
		c.maybeDeleteOldest()

		return nil
	}

	c.lru.MoveToBack(le)
	entry := le.Value.(*entry[K, V])
	if c.maxAge > 0 && c.updateAgeOnGet {
		entry.expires = time.Now().Unix() + c.maxAge
	}
	return entry
}

// Delete removes the value associated with a key.
func (c *LruCache[K, V]) Delete(key K) {
	c.mu.Lock()

	if le, ok := c.cache[key]; ok {
		c.deleteElement(le)
	}

	c.mu.Unlock()
}

// Oldest return oldest element
func (c *LruCache[K, V]) Oldest() (K, V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elm := c.lru.Front()
	if elm == nil {
		return *new(K), *new(V), false
	}

	entry := elm.Value.(*entry[K, V])
	return entry.key, entry.value, true
}

func (c *LruCache[K, V]) maybeDeleteOldest() {
	if !c.staleReturn && c.maxAge > 0 {
		now := time.Now().Unix()
		for le := c.lru.Front(); le != nil && le.Value.(*entry[K, V]).expires <= now; le = c.lru.Front() {
			c.deleteElement(le)
		}
	}
}

func (c *LruCache[K, V]) deleteElement(le *list.Element) {
	c.lru.Remove(le)
	e := le.Value.(*entry[K, V])
	delete(c.cache, e.key)
	if c.onEvict != nil {
		c.onEvict(e.key, e.value)
	}
}

type entry[K comparable, V any] struct {
	key     K
	value   V
	expires int64
}