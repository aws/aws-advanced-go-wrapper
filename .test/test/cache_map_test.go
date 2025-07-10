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
	"testing"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/stretchr/testify/assert"
)

func TestCacheMap_PutAndGet(t *testing.T) {
	cache := utils.NewCache[string]()
	key := "key"
	value := "val"

	cache.Put(key, value, time.Minute)
	got, ok := cache.Get(key)

	assert.True(t, ok)
	assert.Equal(t, value, got)
}

func TestCacheMap_ExpiredGet(t *testing.T) {
	cache := utils.NewCache[string]()
	cache.Put("expired", "value", -1*time.Second)

	got, ok := cache.Get("expired")
	assert.False(t, ok)
	assert.Empty(t, got)
}

func TestCacheMap_ComputeIfAbsent(t *testing.T) {
	cache := utils.NewCache[int]()

	v := cache.ComputeIfAbsent("num", func() int {
		return 1
	}, time.Minute)
	assert.Equal(t, 1, v)

	v2 := cache.ComputeIfAbsent("num", func() int {
		return 99 // shouldn't be called
	}, time.Minute)
	assert.Equal(t, 1, v2)
}

func TestCacheMap_PutIfAbsent(t *testing.T) {
	cache := utils.NewCache[string]()

	cache.PutIfAbsent("a", "first", time.Minute)
	cache.PutIfAbsent("a", "second", time.Minute)

	v, ok := cache.Get("a")
	assert.True(t, ok)
	assert.Equal(t, "first", v)
}

func TestCacheMap_Remove(t *testing.T) {
	cache := utils.NewCache[string]()
	cache.Put("rm", "gone", time.Minute)

	cache.Remove("rm")
	_, ok := cache.Get("rm")
	assert.False(t, ok)
}

func TestCacheMap_Clear(t *testing.T) {
	cache := utils.NewCache[string]()
	cache.Put("k1", "v1", time.Minute)
	cache.Put("k2", "v2", time.Minute)

	cache.Clear()
	assert.Equal(t, 0, cache.Size())
}

func TestCacheMap_GetAllEntries(t *testing.T) {
	cache := utils.NewCache[string]()
	cache.Put("a", "1", time.Minute)
	cache.Put("b", "2", time.Minute)

	all := cache.GetAllEntries()
	assert.Len(t, all, 2)
	assert.Equal(t, "1", all["a"])
	assert.Equal(t, "2", all["b"])
}

func TestCacheMap_Size(t *testing.T) {
	cache := utils.NewCache[string]()
	cache.Put("x", "y", time.Minute)
	assert.Equal(t, 1, cache.Size())

	cache.Remove("x")
	assert.Equal(t, 0, cache.Size())
}

func TestCacheMap_CleanUpExpired(t *testing.T) {
	cache := utils.NewCache[string]()

	cache.Put("old", "value", -1*time.Second)

	time.Sleep(10 * time.Millisecond)
	cache.Put("dummy", "val", time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	_, ok := cache.Get("old")
	assert.False(t, ok)
}
