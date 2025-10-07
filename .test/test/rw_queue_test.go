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

package test

import (
	"sync"
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/stretchr/testify/assert"
)

func TestRWQueue_NewQueue(t *testing.T) {
	queue := utils.NewRWQueue[int]()
	assert.NotNil(t, queue)
	assert.True(t, queue.IsEmpty())
	assert.Equal(t, 0, queue.Size())
}

func TestRWQueue_EnqueueDequeue(t *testing.T) {
	queue := utils.NewRWQueue[int]()
	
	queue.Enqueue(1)
	assert.False(t, queue.IsEmpty())
	assert.Equal(t, 1, queue.Size())
	
	queue.Enqueue(2)
	queue.Enqueue(3)
	assert.Equal(t, 3, queue.Size())
	
	item, ok := queue.Dequeue()
	assert.True(t, ok)
	assert.Equal(t, 1, item)
	assert.Equal(t, 2, queue.Size())
	
	item, ok = queue.Dequeue()
	assert.True(t, ok)
	assert.Equal(t, 2, item)
	
	item, ok = queue.Dequeue()
	assert.True(t, ok)
	assert.Equal(t, 3, item)
	assert.True(t, queue.IsEmpty())
}

func TestRWQueue_DequeueEmpty(t *testing.T) {
	queue := utils.NewRWQueue[int]()
	
	item, ok := queue.Dequeue()
	assert.False(t, ok)
	assert.Equal(t, 0, item) // zero value for int
}

func TestRWQueue_StringType(t *testing.T) {
	queue := utils.NewRWQueue[string]()
	
	queue.Enqueue("hello")
	queue.Enqueue("world")
	
	item, ok := queue.Dequeue()
	assert.True(t, ok)
	assert.Equal(t, "hello", item)
	
	item, ok = queue.Dequeue()
	assert.True(t, ok)
	assert.Equal(t, "world", item)
	
	item, ok = queue.Dequeue()
	assert.False(t, ok)
	assert.Equal(t, "", item) // zero value for string
}

func TestRWQueue_Concurrent(t *testing.T) {
	queue := utils.NewRWQueue[int]()
	var wg sync.WaitGroup
	
	// Concurrent enqueues
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			queue.Enqueue(val)
		}(i)
	}
	
	wg.Wait()
	assert.Equal(t, 100, queue.Size())
	
	// Concurrent dequeues
	results := make([]int, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			item, ok := queue.Dequeue()
			assert.True(t, ok)
			results[idx] = item
		}(i)
	}
	
	wg.Wait()
	assert.True(t, queue.IsEmpty())
	assert.Equal(t, 0, queue.Size())
}

func TestRWQueue_ConcurrentReadWrite(t *testing.T) {
	queue := utils.NewRWQueue[int]()
	var wg sync.WaitGroup
	
	// Producer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			queue.Enqueue(i)
		}
	}()
	
	// Consumer goroutine
	consumed := 0
	wg.Add(1)
	go func() {
		defer wg.Done()
		for consumed < 50 {
			if _, ok := queue.Dequeue(); ok {
				consumed++
			}
		}
	}()
	
	wg.Wait()
	assert.True(t, queue.IsEmpty())
}