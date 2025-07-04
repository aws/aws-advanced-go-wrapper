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
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSlidingExpirationCachePutIfAbsent(t *testing.T) {
	slidingExpirationCache := utils.NewSlidingExpirationCache[int]("test")
	slidingExpirationCache.Put("test", 1, time.Second)
	slidingExpirationCache.PutIfAbsent("test", 2, time.Hour)
	val, ok := slidingExpirationCache.Get("test", time.Second)
	if !ok {
		t.Errorf("Get should return the value attached to the given key.")
	}
	assert.Equal(t, 1, val)
}

func TestSlidingExpirationCacheRemove(t *testing.T) {
	slidingExpirationCache := utils.NewSlidingExpirationCache[int]("test")
	slidingExpirationCache.Put("test", 1, time.Hour)
	slidingExpirationCache.Remove("test")
	assert.Equal(t, 0, slidingExpirationCache.Size())
}

func TestSlidingExpirationCacheGetAllEntries(t *testing.T) {
	slidingExpirationCache := utils.NewSlidingExpirationCache[int]("test")
	slidingExpirationCache.Put("a", 1, time.Second)
	slidingExpirationCache.Put("b", 2, time.Hour)

	expectedEntries := make(map[string]int)
	expectedEntries["a"] = 1
	expectedEntries["b"] = 2

	assert.Equal(t, expectedEntries, slidingExpirationCache.GetAllEntries())
}

func TestSlidingExpirationCacheClear(t *testing.T) {
	slidingExpirationCache := utils.NewSlidingExpirationCache[int]("test")
	slidingExpirationCache.Put("a", 1, time.Hour)
	slidingExpirationCache.Put("b", 2, time.Hour)

	slidingExpirationCache.Clear()
	assert.Equal(t, 0, slidingExpirationCache.Size())
}

func TestSlidingExpirationGetExpiredItem(t *testing.T) {
	slidingExpirationCache := utils.NewSlidingExpirationCache[int]("test")
	slidingExpirationCache.Put("a", 1, time.Nanosecond)
	slidingExpirationCache.Put("b", 2, time.Nanosecond)

	item, ok := slidingExpirationCache.Get("a", time.Minute)
	assert.Equal(t, 0, item)
	assert.False(t, ok)
}

func TestSlidingExpirationComputeIfAbsentExpiredItem(t *testing.T) {
	itemDisposed := false
	disposalFunc := func(item int) bool {
		itemDisposed = true
		return true
	}
	slidingExpirationCache := utils.NewSlidingExpirationCache[int]("test", disposalFunc)
	slidingExpirationCache.Put("a", 1, time.Nanosecond)
	slidingExpirationCache.Put("b", 2, time.Nanosecond)

	item := slidingExpirationCache.ComputeIfAbsent("a", func() int { return 3 }, time.Minute)
	assert.Equal(t, 3, item)
	assert.Equal(t, 2, slidingExpirationCache.Size())
	assert.True(t, itemDisposed)
}
