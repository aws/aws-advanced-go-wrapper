/*
  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

  Licensed under the Apache License, Version 2.0 (the "License").
  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package utils

import (
	"sync"
	"time"
)

// ShouldDisposeFunc determines whether an expired item should be disposed during cleanup.
type ShouldDisposeFunc[T any] func(T) bool

// ItemDisposalFunc defines how to dispose of an item when removed.
type ItemDisposalFunc[T any] func(T)

// expirationItem holds a cached value with expiration metadata.
type expirationItem[T any] struct {
	item           T
	expirationTime time.Time
	shouldDispose  ShouldDisposeFunc[T]
}

func (e *expirationItem[T]) isExpired() bool {
	return time.Now().After(e.expirationTime)
}

func (e *expirationItem[T]) shouldCleanup() bool {
	if !e.isExpired() {
		return false
	}
	if e.shouldDispose != nil {
		return e.shouldDispose(e.item)
	}
	return true
}

func (e *expirationItem[T]) extendExpiration(ttl time.Duration) {
	e.expirationTime = time.Now().Add(ttl)
}

// ExpirationCache is a cache where entries expire after a configured TTL.
// Unlike SlidingExpirationCache, expired entries are NOT renewed on access
// unless renewOnAccess is explicitly set to true.
type ExpirationCache[K comparable, V any] struct {
	cache            map[K]*expirationItem[V]
	lock             sync.RWMutex
	ttl              time.Duration
	renewOnAccess    bool
	shouldDispose    ShouldDisposeFunc[V]
	itemDisposalFunc ItemDisposalFunc[V]
}

// ExpirationCacheConfig holds configuration for creating an ExpirationCache.
type ExpirationCacheConfig[V any] struct {
	TTL              time.Duration
	RenewOnAccess    bool
	ShouldDispose    ShouldDisposeFunc[V]
	ItemDisposalFunc ItemDisposalFunc[V]
}

// NewExpirationCache creates a new expiration cache with the given configuration.
func NewExpirationCache[K comparable, V any](config ExpirationCacheConfig[V]) *ExpirationCache[K, V] {
	ttl := config.TTL
	if ttl == 0 {
		ttl = 5 * time.Minute // default TTL
	}
	return &ExpirationCache[K, V]{
		cache:            make(map[K]*expirationItem[V]),
		ttl:              ttl,
		renewOnAccess:    config.RenewOnAccess,
		shouldDispose:    config.ShouldDispose,
		itemDisposalFunc: config.ItemDisposalFunc,
	}
}

// Put stores a value at the given key, disposing of any previous value.
func (c *ExpirationCache[K, V]) Put(key K, value V) *V {
	c.lock.Lock()
	prev := c.cache[key]
	c.cache[key] = &expirationItem[V]{
		item:           value,
		expirationTime: time.Now().Add(c.ttl),
		shouldDispose:  c.shouldDispose,
	}
	c.lock.Unlock()

	if prev != nil && c.itemDisposalFunc != nil {
		c.itemDisposalFunc(prev.item)
		return &prev.item
	}
	if prev != nil {
		return &prev.item
	}
	return nil
}

// Get retrieves the value at the given key.
// Returns nil if not found or expired (unless renewOnAccess is true).
func (c *ExpirationCache[K, V]) Get(key K) (V, bool) {
	var zero V

	if c.renewOnAccess {
		// Need write lock to extend expiration
		c.lock.Lock()
		item := c.cache[key]
		if item == nil {
			c.lock.Unlock()
			return zero, false
		}
		item.extendExpiration(c.ttl)
		result := item.item
		c.lock.Unlock()
		return result, true
	}

	// Read-only path for non-renewable access
	c.lock.RLock()
	item := c.cache[key]
	if item == nil {
		c.lock.RUnlock()
		return zero, false
	}
	if item.isExpired() {
		c.lock.RUnlock()
		return zero, false
	}
	result := item.item
	c.lock.RUnlock()
	return result, true
}

// ComputeIfAbsent returns the existing value or computes and stores a new one.
// If the existing value is expired and renewOnAccess is false, computes a new value.
func (c *ExpirationCache[K, V]) ComputeIfAbsent(key K, computeFunc func() V) V {
	c.lock.Lock()
	defer c.lock.Unlock()

	item := c.cache[key]

	// Return existing if present and (not expired OR renewable)
	if item != nil {
		if c.renewOnAccess {
			item.extendExpiration(c.ttl)
			return item.item
		}
		if !item.isExpired() {
			return item.item
		}
		// Expired and non-renewable - dispose and replace
		if c.itemDisposalFunc != nil {
			c.itemDisposalFunc(item.item)
		}
	}

	// Compute new value
	newValue := computeFunc()
	c.cache[key] = &expirationItem[V]{
		item:           newValue,
		expirationTime: time.Now().Add(c.ttl),
		shouldDispose:  c.shouldDispose,
	}
	return newValue
}

// Exists returns true if a non-expired value exists at the given key.
func (c *ExpirationCache[K, V]) Exists(key K) bool {
	c.lock.RLock()
	item := c.cache[key]
	c.lock.RUnlock()

	return item != nil && !item.shouldCleanup()
}

// Remove removes and disposes of the value at the given key.
func (c *ExpirationCache[K, V]) Remove(key K) *V {
	c.lock.Lock()
	item := c.cache[key]
	delete(c.cache, key)
	c.lock.Unlock()

	if item == nil {
		return nil
	}

	if c.itemDisposalFunc != nil {
		c.itemDisposalFunc(item.item)
	}
	return &item.item
}

// RemoveExpiredEntries removes all expired entries that should be disposed.
func (c *ExpirationCache[K, V]) RemoveExpiredEntries() {
	c.lock.Lock()
	var toDispose []V
	for key, item := range c.cache {
		if item.shouldCleanup() {
			toDispose = append(toDispose, item.item)
			delete(c.cache, key)
		}
	}
	c.lock.Unlock()

	if c.itemDisposalFunc != nil {
		for _, item := range toDispose {
			c.itemDisposalFunc(item)
		}
	}
}

// Clear removes and disposes of all entries.
func (c *ExpirationCache[K, V]) Clear() {
	c.lock.Lock()
	entries := c.cache
	c.cache = make(map[K]*expirationItem[V])
	c.lock.Unlock()

	if c.itemDisposalFunc != nil {
		for _, item := range entries {
			c.itemDisposalFunc(item.item)
		}
	}
}

// GetEntries returns a copy of all entries, including expired ones.
func (c *ExpirationCache[K, V]) GetEntries() map[K]V {
	c.lock.RLock()
	defer c.lock.RUnlock()

	entries := make(map[K]V, len(c.cache))
	for k, v := range c.cache {
		entries[k] = v.item
	}
	return entries
}

// Size returns the number of entries, including expired ones.
func (c *ExpirationCache[K, V]) Size() int {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return len(c.cache)
}
