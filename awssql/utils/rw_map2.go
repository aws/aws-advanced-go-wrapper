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

import "sync"

type RWMap2[K comparable, V any] struct {
	cache        map[K]V
	disposalFunc DisposalFunc[V]
	lock         sync.RWMutex
}

func NewRWMap2[K comparable, V any]() *RWMap2[K, V] {
	return &RWMap2[K, V]{
		cache: make(map[K]V),
	}
}

func NewRWMap2WithDisposalFunc[K comparable, V any](disposalFunc DisposalFunc[V]) *RWMap2[K, V] {
	return &RWMap2[K, V]{
		cache:        make(map[K]V),
		disposalFunc: disposalFunc,
	}
}

func NewRWMap2FromCopy[K comparable, V any](rwMap *RWMap2[K, V]) *RWMap2[K, V] {
	if rwMap == nil {
		return NewRWMap2[K, V]()
	}
	return NewRWMap2FromMap(rwMap.GetAllEntries())
}

func NewRWMap2FromMap[K comparable, V any](mapForCache map[K]V) *RWMap2[K, V] {
	return &RWMap2[K, V]{
		cache: mapForCache,
	}
}

func (c *RWMap2[K, V]) Put(key K, value V) {
	c.lock.Lock()
	defer c.lock.Unlock()
	val, ok := c.cache[key]
	if ok && c.disposalFunc != nil {
		c.disposalFunc(val)
	}
	c.cache[key] = value
}

func (c *RWMap2[K, V]) Get(key K) (V, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	val, ok := c.cache[key]
	return val, ok
}

func (c *RWMap2[K, V]) ComputeIfAbsent(key K, computeFunc func() V) V {
	c.lock.Lock()
	defer c.lock.Unlock()

	item, ok := c.cache[key]
	if ok {
		return item
	}

	c.cache[key] = computeFunc()
	return c.cache[key]
}

func (c *RWMap2[K, V]) PutIfAbsent(key K, value V) {
	c.lock.Lock()
	defer c.lock.Unlock()
	_, ok := c.cache[key]

	if !ok {
		c.cache[key] = value
	}
}

func (c *RWMap2[K, V]) Remove(key K) {
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

func (c *RWMap2[K, V]) Clear() {
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

func (c *RWMap2[K, V]) clearWithDisposalFunc() {
	c.lock.Lock()
	defer c.lock.Unlock()

	for key, value := range c.cache {
		c.disposalFunc(value)
		delete(c.cache, key)
	}
}

func (c *RWMap2[K, V]) GetAllEntries() map[K]V {
	c.lock.RLock()
	defer c.lock.RUnlock()

	entryMap := make(map[K]V)
	for key, value := range c.cache {
		entryMap[key] = value
	}
	return entryMap
}

func (c *RWMap2[K, V]) ReplaceCacheWithCopy(mapToCopy *RWMap2[K, V]) {
	entryMap := mapToCopy.GetAllEntries()
	c.ReplaceCacheWithMap(entryMap)
}

func (c *RWMap2[K, V]) ReplaceCacheWithMap(entryMap map[K]V) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.disposalFunc != nil {
		for _, value := range c.cache {
			c.disposalFunc(value)
		}
	}
	c.cache = entryMap
}

func (c *RWMap2[K, V]) Size() int {
	if c == nil {
		return 0
	}
	c.lock.RLock()
	defer c.lock.RUnlock()

	return len(c.cache)
}

// ProcessAndRemoveIf executes the provided function on each key-value pair
// where the key satisfies the condition function. Matching entries are removed after processing.
func (c *RWMap2[K, V]) ProcessAndRemoveIf(condition func(K) bool, processor func(K, V)) {
	c.lock.Lock()
	defer c.lock.Unlock()

	for key, value := range c.cache {
		if condition(key) {
			processor(key, value)
			if c.disposalFunc != nil {
				c.disposalFunc(value)
			}
			delete(c.cache, key)
		}
	}
}
