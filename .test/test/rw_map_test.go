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
	"fmt"
	"sync"
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/stretchr/testify/assert"
)

func TestNewRWMap(t *testing.T) {
	rwMap := utils.NewRWMap[int]()
	assert.NotNil(t, rwMap, "NewRWMap should return a non-nil map")
	assert.Equal(t, 0, rwMap.Size(), "New map should be empty")
}

func TestRWMapPutAndGet(t *testing.T) {
	rwMap := utils.NewRWMap[string]()

	rwMap.Put("key1", "value1")
	value, ok := rwMap.Get("key1")
	assert.True(t, ok, "Should find the key that was put")
	assert.Equal(t, "value1", value, "Should return the correct value")

	value, ok = rwMap.Get("nonexistent")
	assert.False(t, ok, "Should not find non-existent key")
	assert.Equal(t, "", value, "Should return zero value for non-existent key")

	rwMap.Put("key1", "newvalue1")
	value, ok = rwMap.Get("key1")
	assert.True(t, ok, "Should find the key after overwrite")
	assert.Equal(t, "newvalue1", value, "Should return the new value")

	// Test empty key
	rwMap.Put("", "empty-key-value")
	value, ok = rwMap.Get("")
	assert.True(t, ok, "Should handle empty key")
	assert.Equal(t, "empty-key-value", value, "Should store and retrieve empty key value")

	// Test nil value (for pointer types)
	ptrMap := utils.NewRWMap[*string]()
	ptrMap.Put("nil-value", nil)
	ptrValue, ok := ptrMap.Get("nil-value")
	assert.True(t, ok, "Should handle nil pointer value")
	assert.Nil(t, ptrValue, "Should store and retrieve nil pointer")

	// Test zero value
	intMap := utils.NewRWMap[int]()
	intMap.Put("zero", 0)
	intValue, ok := intMap.Get("zero")
	assert.True(t, ok, "Should handle zero value")
	assert.Equal(t, 0, intValue, "Should store and retrieve zero value")

	// Test very long key
	longKey := string(make([]byte, 10000))
	for i := range longKey {
		longKey = longKey[:i] + "a" + longKey[i+1:]
	}
	rwMap.Put(longKey, "long-key-value")
	value, ok = rwMap.Get(longKey)
	assert.True(t, ok, "Should handle very long key")
	assert.Equal(t, "long-key-value", value, "Should store and retrieve long key value")
}

func TestRWMapComputeIfAbsent(t *testing.T) {
	rwMap := utils.NewRWMap[int]()

	computeCallCount := 0
	computeFunc := func() int {
		computeCallCount++
		return 100
	}

	value := rwMap.ComputeIfAbsent("key1", computeFunc)
	assert.Equal(t, 100, value, "Should return computed value")
	assert.Equal(t, 1, computeCallCount, "Compute function should be called once")

	storedValue, ok := rwMap.Get("key1")
	assert.True(t, ok, "Computed value should be stored")
	assert.Equal(t, 100, storedValue, "Stored value should match computed value")

	value = rwMap.ComputeIfAbsent("key1", computeFunc)
	assert.Equal(t, 100, value, "Should return existing value")
	assert.Equal(t, 1, computeCallCount, "Compute function should not be called again")

	value = rwMap.ComputeIfAbsent("key2", func() int { return 200 })
	assert.Equal(t, 200, value, "Should return new computed value")
	assert.Equal(t, 2, rwMap.Size(), "Map should contain both keys")
}

func TestRWMapPutIfAbsent(t *testing.T) {
	rwMap := utils.NewRWMap[string]()

	rwMap.PutIfAbsent("key1", "value1")
	value, ok := rwMap.Get("key1")
	assert.True(t, ok, "Should find the key that was put")
	assert.Equal(t, "value1", value, "Should return the correct value")

	rwMap.PutIfAbsent("key1", "newvalue1")
	value, ok = rwMap.Get("key1")
	assert.True(t, ok, "Should still find the key")
	assert.Equal(t, "value1", value, "Should return the original value, not the new one")

	rwMap.PutIfAbsent("key2", "value2")
	value, ok = rwMap.Get("key2")
	assert.True(t, ok, "Should find the second key")
	assert.Equal(t, "value2", value, "Should return the correct value for second key")

	assert.Equal(t, 2, rwMap.Size(), "Map should contain both keys")
}

func TestRWMapRemove(t *testing.T) {
	rwMap := utils.NewRWMap[string]()

	rwMap.Put("key1", "value1")
	rwMap.Put("key2", "value2")
	rwMap.Put("key3", "value3")
	assert.Equal(t, 3, rwMap.Size(), "Map should contain 3 items")

	rwMap.Remove("key2")
	assert.Equal(t, 2, rwMap.Size(), "Map should contain 2 items after removal")

	_, ok := rwMap.Get("key2")
	assert.False(t, ok, "Removed key should not be found")

	value, ok := rwMap.Get("key1")
	assert.True(t, ok, "Other keys should still exist")
	assert.Equal(t, "value1", value, "Other values should be unchanged")

	value, ok = rwMap.Get("key3")
	assert.True(t, ok, "Other keys should still exist")
	assert.Equal(t, "value3", value, "Other values should be unchanged")

	rwMap.Remove("nonexistent")
	assert.Equal(t, 2, rwMap.Size(), "Size should remain unchanged when removing non-existent key")
}

func TestRWMapClear(t *testing.T) {
	rwMap := utils.NewRWMap[int]()

	rwMap.Put("key1", 1)
	rwMap.Put("key2", 2)
	rwMap.Put("key3", 3)
	assert.Equal(t, 3, rwMap.Size(), "Map should contain 3 items")

	rwMap.Clear()
	assert.Equal(t, 0, rwMap.Size(), "Map should be empty after clear")

	_, ok := rwMap.Get("key1")
	assert.False(t, ok, "All keys should be removed after clear")
	_, ok = rwMap.Get("key2")
	assert.False(t, ok, "All keys should be removed after clear")
	_, ok = rwMap.Get("key3")
	assert.False(t, ok, "All keys should be removed after clear")

	rwMap.Clear()
	assert.Equal(t, 0, rwMap.Size(), "Clearing empty map should not cause issues")
}

func TestRWMapClearWithDisposalFunc(t *testing.T) {
	disposedValues := make([]int, 0)
	disposalFunc := func(value int) bool {
		disposedValues = append(disposedValues, value)
		return true
	}
	rwMap := utils.NewRWMapWithDisposalFunc[int](disposalFunc)

	rwMap.Put("key1", 1)
	rwMap.Put("key2", 2)
	rwMap.Put("key3", 3)

	rwMap.Clear()

	assert.Equal(t, 0, rwMap.Size(), "Map should be empty after clear")

	assert.Len(t, disposedValues, 3, "Disposal function should be called for all values")
	assert.Contains(t, disposedValues, 1, "Should dispose value 1")
	assert.Contains(t, disposedValues, 2, "Should dispose value 2")
	assert.Contains(t, disposedValues, 3, "Should dispose value 3")
}

func TestRWMapGetAllEntries(t *testing.T) {
	rwMap := utils.NewRWMap[string]()

	entries := rwMap.GetAllEntries()
	assert.Empty(t, entries, "Empty map should return empty entries")

	rwMap.Put("key1", "value1")
	rwMap.Put("key2", "value2")
	rwMap.Put("key3", "value3")

	entries = rwMap.GetAllEntries()
	assert.Len(t, entries, 3, "Should return all entries")
	assert.Equal(t, "value1", entries["key1"], "Should contain correct value for key1")
	assert.Equal(t, "value2", entries["key2"], "Should contain correct value for key2")
	assert.Equal(t, "value3", entries["key3"], "Should contain correct value for key3")

	entries["key4"] = "value4"
	_, ok := rwMap.Get("key4")
	assert.False(t, ok, "Modifying returned entries should not affect original map")
	assert.Equal(t, 3, rwMap.Size(), "Original map size should be unchanged")
}

func TestRWMapReplaceCacheWithCopy(t *testing.T) {
	sourceMap := utils.NewRWMap[int]()
	sourceMap.Put("key1", 1)
	sourceMap.Put("key2", 2)
	sourceMap.Put("key3", 3)

	targetMap := utils.NewRWMap[int]()
	targetMap.Put("oldkey1", 10)
	targetMap.Put("oldkey2", 20)

	targetMap.ReplaceCacheWithCopy(sourceMap)

	assert.Equal(t, 3, targetMap.Size(), "Target map should have source map size")

	value, ok := targetMap.Get("key1")
	assert.True(t, ok, "Target should contain source keys")
	assert.Equal(t, 1, value, "Target should contain source values")

	value, ok = targetMap.Get("key2")
	assert.True(t, ok, "Target should contain source keys")
	assert.Equal(t, 2, value, "Target should contain source values")

	value, ok = targetMap.Get("key3")
	assert.True(t, ok, "Target should contain source keys")
	assert.Equal(t, 3, value, "Target should contain source values")

	_, ok = targetMap.Get("oldkey1")
	assert.False(t, ok, "Old keys should be removed")
	_, ok = targetMap.Get("oldkey2")
	assert.False(t, ok, "Old keys should be removed")

	assert.Equal(t, 3, sourceMap.Size(), "Source map should be unchanged")
}

func TestRWMapSize(t *testing.T) {
	rwMap := utils.NewRWMap[string]()

	assert.Equal(t, 0, rwMap.Size(), "Empty map should have size 0")

	rwMap.Put("key1", "value1")
	assert.Equal(t, 1, rwMap.Size(), "Map should have size 1 after adding one item")

	rwMap.Put("key2", "value2")
	assert.Equal(t, 2, rwMap.Size(), "Map should have size 2 after adding two items")

	rwMap.Put("key1", "newvalue1")
	assert.Equal(t, 2, rwMap.Size(), "Map size should not change when overwriting")

	rwMap.Remove("key1")
	assert.Equal(t, 1, rwMap.Size(), "Map should have size 1 after removing one item")

	rwMap.Remove("key2")
	assert.Equal(t, 0, rwMap.Size(), "Map should have size 0 after removing all items")

	rwMap.Remove("nonexistent")
	assert.Equal(t, 0, rwMap.Size(), "Map size should not change when removing non-existent key")
}

func TestRWMapConcurrency(t *testing.T) {
	rwMap := utils.NewRWMap[int]()
	numGoroutines := 100
	numOperationsPerGoroutine := 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 3)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperationsPerGoroutine; j++ {
				key := fmt.Sprintf("writer-%d-%d", id, j)
				rwMap.Put(key, id*1000+j)
			}
		}(i)
	}

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperationsPerGoroutine; j++ {
				key := fmt.Sprintf("writer-%d-%d", id%10, j%10)
				rwMap.Get(key)
			}
		}(i)
	}

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperationsPerGoroutine; j++ {
				key := fmt.Sprintf("compute-%d-%d", id, j)
				rwMap.ComputeIfAbsent(key, func() int { return id*2000 + j })
			}
		}(i)
	}

	wg.Wait()

	size := rwMap.Size()
	assert.True(t, size > 0, "Map should contain items after concurrent operations")

	entries := rwMap.GetAllEntries()
	assert.Equal(t, size, len(entries), "GetAllEntries should return consistent number of items")
}
