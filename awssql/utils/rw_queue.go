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

type RWQueue[T any] struct {
	items []T
	lock  sync.RWMutex
}

func NewRWQueue[T any]() *RWQueue[T] {
	return &RWQueue[T]{
		items: make([]T, 0),
	}
}

func (q *RWQueue[T]) Enqueue(item T) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.items = append(q.items, item)
}

func (q *RWQueue[T]) Dequeue() (T, bool) {
	q.lock.Lock()
	defer q.lock.Unlock()

	var zero T
	if len(q.items) == 0 {
		return zero, false
	}

	item := q.items[0]
	q.items = q.items[1:]
	return item, true
}

func (q *RWQueue[T]) Size() int {
	q.lock.RLock()
	defer q.lock.RUnlock()
	return len(q.items)
}

func (q *RWQueue[T]) IsEmpty() bool {
	q.lock.RLock()
	defer q.lock.RUnlock()
	return len(q.items) == 0
}

func (q *RWQueue[T]) ForEach(fn func(T)) {
	q.lock.RLock()
	defer q.lock.RUnlock()
	for _, item := range q.items {
		fn(item)
	}
}

func (q *RWQueue[T]) RemoveIf(condition func(T) bool) {
	q.lock.Lock()
	defer q.lock.Unlock()

	filtered := make([]T, 0, len(q.items))
	for _, item := range q.items {
		if !condition(item) {
			filtered = append(filtered, item)
		}
	}
	q.items = filtered
}
