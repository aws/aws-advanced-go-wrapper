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
	cache        map[string]T
	disposalFunc DisposalFunc[T]
	lock         sync.RWMutex
}

func NewRWMap[T any]() *RWMap[T] {
	return &RWMap[T]{
		cache: make(map[string]T),
	}
}

func NewRWMapWithDisposalFunc[T any](disposalFunc DisposalFunc[T]) *RWMap[T] {
	return &RWMap[T]{
		cache:        make(map[string]T),
		disposalFunc: disposalFunc,
	}
}

func NewRWMapFromCopy[T any](rwMap *RWMap[T]) *RWMap[T] {
	if rwMap == nil {
		return NewRWMap[T]()
	}
	return NewRWMapFromMap(rwMap.GetAllEntries())
}

func NewRWMapFromMap[T any](mapForCache map[string]T) *RWMap[T] {
	return &RWMap[T]{
		cache: mapForCache,
	}
}

func (c *RWMap[T]) Put(key string, value T) {
	c.lock.Lock()
	defer c.lock.Unlock()
	val, ok := c.cache[key]
	if ok && c.disposalFunc != nil {
		c.disposalFunc(val)
	}
	c.cache[key] = value
}

func (c *RWMap[T]) Get(key string) (T, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	val, ok := c.cache[key]
	return val, ok
}

func (c *RWMap[T]) ComputeIfAbsent(key string, computeFunc func() T) T {
	c.lock.Lock()
	defer c.lock.Unlock()

	item, ok := c.cache[key]
	if ok {
		return item
	}

	c.cache[key] = computeFunc()
	return c.cache[key]
}

func (c *RWMap[T]) PutIfAbsent(key string, value T) {
	c.lock.Lock()
	defer c.lock.Unlock()
	_, ok := c.cache[key]

	if !ok {
		c.cache[key] = value
	}
}

func (c *RWMap[T]) Remove(key string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	val, ok := c.cache[key]
	if ok {
		if c.disposalFunc != nil {
			c.disposalFunc(val)
		}
		delete(c.cache, key)
	}
}

func (c *RWMap[T]) Clear() {
	if c.disposalFunc != nil {
		c.clearWithDisposalFunc()
		return
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	for key := range c.cache {
		delete(c.cache, key)
	}
}

func (c *RWMap[T]) clearWithDisposalFunc() {
	c.lock.Lock()
	defer c.lock.Unlock()

	for key, value := range c.cache {
		c.disposalFunc(value)
		delete(c.cache, key)
	}
}

// Returns a map copy of all entries in the cache.
func (c *RWMap[T]) GetAllEntries() map[string]T {
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

	c.ReplaceCacheWithMap(entryMap)
}

func (c *RWMap[T]) ReplaceCacheWithMap(entryMap map[string]T) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.disposalFunc != nil {
		for _, value := range c.cache {
			c.disposalFunc(value)
		}
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
