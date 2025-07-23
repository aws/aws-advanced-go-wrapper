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
)

type RWMap[T any] struct {
	cache map[string]T
	lock  sync.RWMutex
}

func NewRWMap[T any]() *RWMap[T] {
	return &RWMap[T]{
		cache: make(map[string]T),
	}
}

func (c *RWMap[T]) Put(key string, value T) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.cache[key] = value
}

func (c *RWMap[T]) Get(key string) (t T, ok bool) {
	if c == nil {
		return
	}
	c.lock.RLock()
	defer c.lock.RUnlock()

	val, ok := c.cache[key]
	return val, ok
}

func (c *RWMap[T]) ComputeIfAbsent(key string, computeFunc func() T) T {
	c.lock.RLock()
	item, ok := c.cache[key]
	c.lock.RUnlock()

	if ok {
		return item
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	c.cache[key] = computeFunc()
	return c.cache[key]
}

func (c *RWMap[T]) PutIfAbsent(key string, value T) {
	c.lock.RLock()
	_, ok := c.cache[key]
	c.lock.RUnlock()

	if !ok {
		c.Put(key, value)
	}
}

func (c *RWMap[T]) Remove(key string) {
	if c == nil {
		return
	}
	c.lock.Lock()
	delete(c.cache, key)
	c.lock.Unlock()
}

func (c *RWMap[T]) Clear() {
	if c == nil {
		return
	}
	c.lock.Lock()
	defer c.lock.Unlock()

	for k := range c.cache {
		delete(c.cache, k)
	}
}

func (c *RWMap[T]) ClearWithDisposalFunc(disposalFunc DisposalFunc[T]) {
	if c == nil {
		return
	}
	c.lock.Lock()
	defer c.lock.Unlock()

	for k, v := range c.cache {
		disposalFunc(v)
		delete(c.cache, k)
	}
}

// GetAllEntries Returns a map copy of all entries in the cache.
func (c *RWMap[T]) GetAllEntries() map[string]T {
	if c == nil {
		return make(map[string]T)
	}
	c.lock.RLock()
	defer c.lock.RUnlock()

	entryMap := make(map[string]T)
	for key, value := range c.cache {
		entryMap[key] = value
	}
	return entryMap
}

func (c *RWMap[T]) ReplaceCacheWithCopy(mapToCopy *RWMap[T]) {
	entryMap := mapToCopy.GetAllEntries()

	c.lock.Lock()
	defer c.lock.Unlock()
	for k := range c.cache {
		delete(c.cache, k)
	}
	c.cache = entryMap
}

func (c *RWMap[T]) Size() int {
	if c == nil {
		return 0
	}
	c.lock.RLock()
	defer c.lock.RUnlock()

	return len(c.cache)
}
