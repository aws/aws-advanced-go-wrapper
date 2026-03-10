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

package services

import (
	"testing"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/services"
	"github.com/stretchr/testify/assert"
)

const testTypeKey = "test-type"

func newTestStorage(t *testing.T) *services.ExpiringStorage {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	t.Cleanup(func() { publisher.Stop() })
	storage := services.NewExpiringStorage(1*time.Hour, publisher)
	t.Cleanup(func() { storage.Stop() })
	return storage
}

func TestStorageRegisterAndSet(t *testing.T) {
	storage := newTestStorage(t)
	storage.Register(testTypeKey, 5*time.Minute, false, nil, nil)

	storage.Set(testTypeKey, "key1", "value1")
	val, ok := storage.Get(testTypeKey, "key1")
	assert.True(t, ok)
	assert.Equal(t, "value1", val)
}

func TestStorageGetNonExistentKey(t *testing.T) {
	storage := newTestStorage(t)
	storage.Register(testTypeKey, 5*time.Minute, false, nil, nil)

	val, ok := storage.Get(testTypeKey, "nonexistent")
	assert.False(t, ok)
	assert.Nil(t, val)
}

func TestStorageGetUnregisteredType(t *testing.T) {
	storage := newTestStorage(t)

	val, ok := storage.Get("unregistered", "key1")
	assert.False(t, ok)
	assert.Nil(t, val)
}

func TestStorageSetUnregisteredType(t *testing.T) {
	storage := newTestStorage(t)

	// Should not panic, just silently ignore
	storage.Set("unregistered", "key1", "value1")
	val, ok := storage.Get("unregistered", "key1")
	assert.False(t, ok)
	assert.Nil(t, val)
}

func TestStorageExists(t *testing.T) {
	storage := newTestStorage(t)
	storage.Register(testTypeKey, 5*time.Minute, false, nil, nil)

	assert.False(t, storage.Exists(testTypeKey, "key1"))

	storage.Set(testTypeKey, "key1", "value1")
	assert.True(t, storage.Exists(testTypeKey, "key1"))
}

func TestStorageExistsUnregisteredType(t *testing.T) {
	storage := newTestStorage(t)
	assert.False(t, storage.Exists("unregistered", "key1"))
}

func TestStorageRemove(t *testing.T) {
	storage := newTestStorage(t)
	storage.Register(testTypeKey, 5*time.Minute, false, nil, nil)

	storage.Set(testTypeKey, "key1", "value1")
	assert.True(t, storage.Exists(testTypeKey, "key1"))

	storage.Remove(testTypeKey, "key1")
	assert.False(t, storage.Exists(testTypeKey, "key1"))
}

func TestStorageRemoveUnregisteredType(t *testing.T) {
	storage := newTestStorage(t)
	// Should not panic
	storage.Remove("unregistered", "key1")
}

func TestStorageClear(t *testing.T) {
	storage := newTestStorage(t)
	storage.Register(testTypeKey, 5*time.Minute, false, nil, nil)

	storage.Set(testTypeKey, "key1", "value1")
	storage.Set(testTypeKey, "key2", "value2")
	assert.Equal(t, 2, storage.Size(testTypeKey))

	storage.Clear(testTypeKey)
	assert.Equal(t, 0, storage.Size(testTypeKey))
}

func TestStorageClearUnregisteredType(t *testing.T) {
	storage := newTestStorage(t)
	// Should not panic
	storage.Clear("unregistered")
}

func TestStorageClearAll(t *testing.T) {
	storage := newTestStorage(t)
	storage.Register("type1", 5*time.Minute, false, nil, nil)
	storage.Register("type2", 5*time.Minute, false, nil, nil)

	storage.Set("type1", "key1", "value1")
	storage.Set("type2", "key2", "value2")

	storage.ClearAll()
	assert.Equal(t, 0, storage.Size("type1"))
	assert.Equal(t, 0, storage.Size("type2"))
}

func TestStorageSize(t *testing.T) {
	storage := newTestStorage(t)
	storage.Register(testTypeKey, 5*time.Minute, false, nil, nil)

	assert.Equal(t, 0, storage.Size(testTypeKey))

	storage.Set(testTypeKey, "key1", "value1")
	assert.Equal(t, 1, storage.Size(testTypeKey))

	storage.Set(testTypeKey, "key2", "value2")
	assert.Equal(t, 2, storage.Size(testTypeKey))
}

func TestStorageSizeUnregisteredType(t *testing.T) {
	storage := newTestStorage(t)
	assert.Equal(t, 0, storage.Size("unregistered"))
}

func TestStorageOverwriteValue(t *testing.T) {
	storage := newTestStorage(t)
	storage.Register(testTypeKey, 5*time.Minute, false, nil, nil)

	storage.Set(testTypeKey, "key1", "value1")
	storage.Set(testTypeKey, "key1", "value2")

	val, ok := storage.Get(testTypeKey, "key1")
	assert.True(t, ok)
	assert.Equal(t, "value2", val)
	assert.Equal(t, 1, storage.Size(testTypeKey))
}

func TestStorageWithDisposalFunc(t *testing.T) {
	disposed := make([]any, 0)
	disposalFunc := func(value any) {
		disposed = append(disposed, value)
	}

	storage := newTestStorage(t)
	storage.Register(testTypeKey, 5*time.Minute, false, nil, disposalFunc)

	storage.Set(testTypeKey, "key1", "value1")
	storage.Remove(testTypeKey, "key1")

	assert.Len(t, disposed, 1)
	assert.Equal(t, "value1", disposed[0])
}

func TestStorageImplementsInterfaces(t *testing.T) {
	var _ driver_infrastructure.StorageService = (*services.ExpiringStorage)(nil)
	var _ driver_infrastructure.RawStorageAccess = (*services.ExpiringStorage)(nil)
}

func TestStoragePublishesDataAccessEvent(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(50 * time.Millisecond)
	defer publisher.Stop()

	sub := &testSubscriber{}
	publisher.Subscribe(sub, []*driver_infrastructure.EventType{services.DataAccessEventType})

	storage := services.NewExpiringStorage(1*time.Hour, publisher)
	defer storage.Stop()
	storage.Register(testTypeKey, 5*time.Minute, false, nil, nil)

	storage.Set(testTypeKey, "key1", "value1")
	storage.Get(testTypeKey, "key1")

	// DataAccessEvent is batched, wait for delivery
	time.Sleep(150 * time.Millisecond)

	events := sub.getEvents()
	assert.GreaterOrEqual(t, len(events), 1)
}

func TestStorageDuplicateRegister(t *testing.T) {
	storage := newTestStorage(t)
	storage.Register(testTypeKey, 5*time.Minute, false, nil, nil)
	// Registering again should not overwrite
	storage.Register(testTypeKey, 10*time.Minute, true, nil, nil)

	storage.Set(testTypeKey, "key1", "value1")
	val, ok := storage.Get(testTypeKey, "key1")
	assert.True(t, ok)
	assert.Equal(t, "value1", val)
}

func TestStorageWithNilPublisher(t *testing.T) {
	storage := services.NewExpiringStorage(1*time.Hour, nil)
	defer storage.Stop()
	storage.Register(testTypeKey, 5*time.Minute, false, nil, nil)

	storage.Set(testTypeKey, "key1", "value1")
	val, ok := storage.Get(testTypeKey, "key1")
	assert.True(t, ok)
	assert.Equal(t, "value1", val)
}

func TestStorageRemoveExpiredItems(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()

	// Use a very short cleanup interval
	storage := services.NewExpiringStorage(50*time.Millisecond, publisher)
	defer storage.Stop()

	// Register with a very short TTL
	storage.Register("expiring-type", 1*time.Millisecond, false, nil, nil)

	storage.Set("expiring-type", "key1", "value1")
	assert.Equal(t, 1, storage.Size("expiring-type"))

	// Wait for TTL to expire and cleanup to run
	time.Sleep(200 * time.Millisecond)

	// The expired entry should have been cleaned up
	val, ok := storage.Get("expiring-type", "key1")
	assert.False(t, ok)
	assert.Nil(t, val)
}

func TestStorageWithShouldDisposeFunc(t *testing.T) {
	disposed := make([]any, 0)
	disposalFunc := func(value any) {
		disposed = append(disposed, value)
	}
	shouldDispose := func(value any) bool {
		// Only dispose values that are "disposable"
		return value == "disposable"
	}

	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()
	storage := services.NewExpiringStorage(50*time.Millisecond, publisher)
	defer storage.Stop()

	storage.Register("dispose-type", 1*time.Millisecond, false, shouldDispose, disposalFunc)

	storage.Set("dispose-type", "key1", "disposable")
	storage.Set("dispose-type", "key2", "not-disposable")

	// Wait for cleanup
	time.Sleep(200 * time.Millisecond)

	// "disposable" should have been disposed, "not-disposable" should still be in cache (size-wise)
	// but expired (not accessible via Get)
	assert.Contains(t, disposed, "disposable")
}

func TestStorageRenewOnAccess(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()
	storage := services.NewExpiringStorage(1*time.Hour, publisher)
	defer storage.Stop()

	// Register with renewOnAccess=true and short TTL
	storage.Register("renew-type", 200*time.Millisecond, true, nil, nil)

	storage.Set("renew-type", "key1", "value1")

	// Access before expiration to renew
	time.Sleep(100 * time.Millisecond)
	val, ok := storage.Get("renew-type", "key1")
	assert.True(t, ok)
	assert.Equal(t, "value1", val)

	// Access again after another 100ms (would have expired without renewal)
	time.Sleep(100 * time.Millisecond)
	val, ok = storage.Get("renew-type", "key1")
	assert.True(t, ok)
	assert.Equal(t, "value1", val)
}
