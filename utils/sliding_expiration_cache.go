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
	"awssql/error_util"
	"context"
	"log/slog"
	"sync"
	"time"
)

type DisposalFunc[T any] func(T) bool

type SlidingExpirationCache[T any] struct {
	cacheId              string
	cache                map[string]*cacheItem[T]
	cleanupIntervalNanos time.Duration
	cleanupTimeNanos     time.Time
	lock                 sync.RWMutex
	itemDisposalFunc     DisposalFunc[T]
	shouldDisposeFunc    DisposalFunc[T]
	cancelCleanup        context.CancelFunc
}

func NewSlidingExpirationCache[T any](id string, funcs ...DisposalFunc[T]) *SlidingExpirationCache[T] {
	ctx, cancel := context.WithCancel(context.Background())

	cache := &SlidingExpirationCache[T]{
		cacheId:              id,
		cache:                make(map[string]*cacheItem[T]),
		cleanupIntervalNanos: CleanupIntervalNanos,
		cleanupTimeNanos:     time.Now().Add(CleanupIntervalNanos),
		cancelCleanup:        cancel,
	}

	if len(funcs) > 0 {
		if len(funcs) > 1 {
			cache.shouldDisposeFunc = funcs[1]
		}
		cache.itemDisposalFunc = funcs[0]
	}

	// Start the cache cleanup goroutine.
	slog.Info(error_util.GetMessage("SlidingExpirationCache.startingCacheCleanupRoutine", id))
	go cache.cleanupExpiredItems(ctx)
	return cache
}

func (c *SlidingExpirationCache[T]) Put(key string, value T, itemExpiration time.Duration) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.cache[key] = &cacheItem[T]{cacheValue[T]{
		item:           value,
		expirationTime: time.Now().Add(itemExpiration),
	}}
}

func (c *SlidingExpirationCache[T]) Get(key string, itemExpiration time.Duration) (T, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	item, ok := c.cache[key]
	if !ok || item.shouldCleanup(c.shouldDisposeFunc) {
		var zeroValue T
		return zeroValue, false
	}

	item.withExtendExpiration(itemExpiration)
	return item.item, true
}

func (c *SlidingExpirationCache[T]) ComputeIfAbsent(key string, computeFunc func() T, itemExpiration time.Duration) T {
	c.lock.RLock()
	item, ok := c.cache[key]
	c.lock.RUnlock()

	if ok {
		return item.item
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	c.cache[key] = &cacheItem[T]{cacheValue[T]{
		item:           computeFunc(),
		expirationTime: time.Now().Add(itemExpiration),
	}}
	c.cache[key].withExtendExpiration(itemExpiration)
	return c.cache[key].item
}

func (c *SlidingExpirationCache[T]) PutIfAbsent(key string, value T, expiration time.Duration) {
	c.lock.Lock()
	_, ok := c.cache[key]
	c.lock.Unlock()

	if !ok {
		c.Put(key, value, expiration)
	}
}

func (c *SlidingExpirationCache[T]) Remove(key string) {
	c.lock.Lock()
	cacheItem, ok := c.cache[key]
	delete(c.cache, key)
	c.lock.Unlock()

	if ok && cacheItem != nil && c.itemDisposalFunc != nil {
		c.itemDisposalFunc(cacheItem.item)
	}
}

func (c *SlidingExpirationCache[T]) Clear() {
	entries := c.GetAllEntries()

	c.lock.Lock()
	c.cache = make(map[string]*cacheItem[T])
	c.cleanupTimeNanos = time.Now().Add(CleanupIntervalNanos)
	c.lock.Unlock()

	if c.itemDisposalFunc != nil {
		for _, item := range entries {
			c.itemDisposalFunc(item)
		}
	}
}

func (c *SlidingExpirationCache[T]) CleanUp() {
	c.Clear()
	c.cancelCleanup()
}

// Get a map copy of all entries in the cache, including expired entries.
func (c *SlidingExpirationCache[T]) GetAllEntries() map[string]T {
	c.lock.Lock()
	defer c.lock.Unlock()

	entryMap := make(map[string]T)
	for key, value := range c.cache {
		entryMap[key] = value.item
	}
	return entryMap
}

func (c *SlidingExpirationCache[T]) Size() int {
	if c == nil {
		return 0
	}
	c.lock.RLock()
	defer c.lock.RUnlock()
	return len(c.cache)
}

func (c *SlidingExpirationCache[T]) SetCleanupIntervalNanos(newIntervalNanos time.Duration) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	c.cleanupIntervalNanos = newIntervalNanos
	c.cleanupTimeNanos = time.Now().Add(newIntervalNanos)
}

func (c *SlidingExpirationCache[T]) cleanupExpiredItems(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			slog.Info(error_util.GetMessage("SlidingExpirationCache.exitingCacheCleanupRoutine", c.cacheId))
			return
		default:
			time.Sleep(CleanupIntervalNanos)

			var itemList []T
			c.lock.Lock()
			for key, item := range c.cache {
				if item.shouldCleanup(c.shouldDisposeFunc) {
					itemList = append(itemList, item.item)
					delete(c.cache, key)
				}
			}
			c.lock.Unlock()

			// Dispose of the items after it has been removed and the cache is unlocked, as the disposal func may be long-running.
			if c.itemDisposalFunc != nil {
				for _, item := range itemList {
					c.itemDisposalFunc(item)
				}
			}
		}
	}
}

type cacheItem[T any] struct {
	cacheValue[T]
}

func (c *cacheItem[T]) withExtendExpiration(itemExpirationNano time.Duration) {
	c.expirationTime = time.Now().Add(itemExpirationNano)
}

func (c *cacheItem[T]) shouldCleanup(shouldDisposeFunc DisposalFunc[T]) bool {
	if shouldDisposeFunc != nil {
		return time.Now().After(c.expirationTime) && shouldDisposeFunc(c.item)
	}
	return time.Now().After(c.expirationTime)
}
