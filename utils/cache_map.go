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
	"fmt"
	"reflect"
	"sync"
	"time"
)

var CleanupIntervalNanos time.Duration = 10 * time.Minute

type CacheMap[T any] struct {
	cache            map[string]cacheValue[T]
	cleanupTimeNanos time.Time
	lock             sync.RWMutex
}

func NewCache[T any]() *CacheMap[T] {
	return &CacheMap[T]{
		cache:            make(map[string]cacheValue[T]),
		cleanupTimeNanos: time.Now().Add(CleanupIntervalNanos),
	}
}

func (c *CacheMap[T]) Put(key string, value T, expiration time.Duration) {
	c.lock.Lock()

	expirationTime := time.Now().Add(expiration)
	c.cache[key] = cacheValue[T]{
		item:           value,
		expirationTime: expirationTime,
	}

	c.lock.Unlock()
	defer c.CleanUp()
}

func (c *CacheMap[T]) Get(key string) (T, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	value, ok := c.cache[key]
	if !ok || value.isExpired() {
		var zeroValue T
		return zeroValue, false
	}

	return value.item, true
}

func (c *CacheMap[T]) ComputeIfAbsent(key string, computeFunc func() T, itemExpiration time.Duration) T {
	c.lock.RLock()
	item, ok := c.cache[key]
	c.lock.RUnlock()

	if ok {
		return item.item
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	c.cache[key] = cacheValue[T]{
		item:           computeFunc(),
		expirationTime: time.Now().Add(itemExpiration),
	}
	return c.cache[key].item
}

func (c *CacheMap[T]) PutIfAbsent(key string, value T, expiration time.Duration) {
	c.lock.RLock()
	_, ok := c.cache[key]
	c.lock.RUnlock()

	if !ok {
		c.Put(key, value, expiration)
	}
	defer c.CleanUp()
}

func (c *CacheMap[T]) Remove(key string) {
	c.lock.Lock()
	delete(c.cache, key)
	c.lock.Unlock()
	defer c.CleanUp()
}

func (c *CacheMap[T]) Clear() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.cache = make(map[string]cacheValue[T])
	c.cleanupTimeNanos = time.Now().Add(CleanupIntervalNanos)
}

// Get a map copy of all entries in the cache, including expired entries.
func (c *CacheMap[T]) GetAllEntries() map[string]T {
	c.lock.RLock()
	defer c.lock.RUnlock()

	entryMap := make(map[string]T)
	for key, value := range c.cache {
		entryMap[key] = value.item
	}
	return entryMap
}

func (c *CacheMap[T]) Size() int {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return len(c.cache)
}

func (c *CacheMap[T]) CleanUp() {
	c.lock.Lock()
	defer c.lock.Unlock()

	if time.Now().After(c.cleanupTimeNanos) {
		for key, value := range c.cache {
			if value.isExpired() {
				delete(c.cache, key)
			}
		}
		c.cleanupTimeNanos = time.Now().Add(CleanupIntervalNanos)
	}
}

type cacheValue[T any] struct {
	item           T
	expirationTime time.Time
}

func (c cacheValue[T]) isExpired() bool {
	return time.Now().After(c.expirationTime)
}

func (c cacheValue[T]) Equals(valueToCompare any) bool {
	if reflect.TypeOf(c.item) != reflect.TypeOf(valueToCompare) {
		return false
	}

	return reflect.DeepEqual(c.item, valueToCompare)
}

func (c cacheValue[T]) String() string {
	return fmt.Sprintf("CacheItem [item= %v, expirationTime= %s]", c.item, c.expirationTime.Format(time.RFC1123))
}
