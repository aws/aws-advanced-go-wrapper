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
	"sync"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

// Default cleanup interval.
const defaultCleanupInterval = 5 * time.Minute

// cacheEntry wraps an ExpirationCache with its configuration.
type cacheEntry struct {
	cache *utils.ExpirationCache[any, any]
}

// ExpiringStorage provides shared, expiring key-value storage for driver components.
// Uses ExpirationCache (non-sliding by default) to match Java's StorageServiceImpl.
// Implements driver_infrastructure.StorageService.
type ExpiringStorage struct {
	caches          map[string]*cacheEntry
	cachesMu        sync.RWMutex
	publisher       driver_infrastructure.EventPublisher
	cleanupInterval time.Duration
	stopCleanup     chan struct{}
}

// NewExpiringStorage creates a new expiring storage.
// Call driver_infrastructure.RegisterDefaultStorageTypes(s) after creation
// to register the built-in storage types.
func NewExpiringStorage(cleanupInterval time.Duration, publisher driver_infrastructure.EventPublisher) *ExpiringStorage {
	if cleanupInterval == 0 {
		cleanupInterval = defaultCleanupInterval
	}

	s := &ExpiringStorage{
		caches:          make(map[string]*cacheEntry),
		publisher:       publisher,
		cleanupInterval: cleanupInterval,
		stopCleanup:     make(chan struct{}),
	}

	go s.cleanupLoop()
	return s
}

// register is the internal registration method.
func (s *ExpiringStorage) register(
	typeKey string,
	ttl time.Duration,
	renewOnAccess bool,
	shouldDispose driver_infrastructure.ShouldDisposeFunc,
	onDispose driver_infrastructure.ItemDisposalFunc,
) {
	var shouldDisposeFunc utils.ShouldDisposeFunc[any]
	var itemDisposalFunc utils.ItemDisposalFunc[any]

	if shouldDispose != nil {
		shouldDisposeFunc = func(v any) bool { return shouldDispose(v) }
	}
	if onDispose != nil {
		itemDisposalFunc = func(v any) { onDispose(v) }
	}

	cache := utils.NewExpirationCache[any, any](utils.ExpirationCacheConfig[any]{
		TTL:              ttl,
		RenewOnAccess:    renewOnAccess,
		ShouldDispose:    shouldDisposeFunc,
		ItemDisposalFunc: itemDisposalFunc,
	})

	s.cachesMu.Lock()
	if _, exists := s.caches[typeKey]; !exists {
		s.caches[typeKey] = &cacheEntry{cache: cache}
	}
	s.cachesMu.Unlock()
}

// Register registers a new item type with the storage service.
// This must be called before storing items of a custom type.
func (s *ExpiringStorage) Register(
	typeKey string,
	ttl time.Duration,
	renewOnAccess bool,
	shouldDispose driver_infrastructure.ShouldDisposeFunc,
	onDispose driver_infrastructure.ItemDisposalFunc,
) {
	s.register(typeKey, ttl, renewOnAccess, shouldDispose, onDispose)
}

// Set stores an item under the given type and key.
func (s *ExpiringStorage) Set(typeKey string, key any, value any) {
	s.cachesMu.RLock()
	entry := s.caches[typeKey]
	s.cachesMu.RUnlock()

	if entry == nil {
		return
	}

	entry.cache.Put(key, value)
}

// Get retrieves an item by type and key. Returns nil if not found or expired.
func (s *ExpiringStorage) Get(typeKey string, key any) (any, bool) {
	s.cachesMu.RLock()
	entry := s.caches[typeKey]
	s.cachesMu.RUnlock()

	if entry == nil {
		return nil, false
	}

	value, ok := entry.cache.Get(key)
	if !ok {
		return nil, false
	}

	// Publish data access event
	if s.publisher != nil {
		s.publisher.Publish(DataAccessEvent{
			TypeKey: typeKey,
			Key:     key,
		})
	}

	return value, true
}

// Exists checks if an item exists (and is not expired).
func (s *ExpiringStorage) Exists(typeKey string, key any) bool {
	s.cachesMu.RLock()
	entry := s.caches[typeKey]
	s.cachesMu.RUnlock()

	if entry == nil {
		return false
	}

	return entry.cache.Exists(key)
}

// Remove removes an item by type and key.
func (s *ExpiringStorage) Remove(typeKey string, key any) {
	s.cachesMu.RLock()
	entry := s.caches[typeKey]
	s.cachesMu.RUnlock()

	if entry == nil {
		return
	}

	entry.cache.Remove(key)
}

// Clear removes all items of the given type.
func (s *ExpiringStorage) Clear(typeKey string) {
	s.cachesMu.RLock()
	entry := s.caches[typeKey]
	s.cachesMu.RUnlock()

	if entry == nil {
		return
	}

	entry.cache.Clear()
}

// ClearAll removes all items from all caches.
func (s *ExpiringStorage) ClearAll() {
	s.cachesMu.RLock()
	entries := make([]*cacheEntry, 0, len(s.caches))
	for _, entry := range s.caches {
		entries = append(entries, entry)
	}
	s.cachesMu.RUnlock()

	for _, entry := range entries {
		entry.cache.Clear()
	}
}

// Size returns the number of items stored under the given type.
func (s *ExpiringStorage) Size(typeKey string) int {
	s.cachesMu.RLock()
	entry := s.caches[typeKey]
	s.cachesMu.RUnlock()

	if entry == nil {
		return 0
	}

	return entry.cache.Size()
}

// Stop stops the background cleanup goroutine.
func (s *ExpiringStorage) Stop() {
	close(s.stopCleanup)
}

func (s *ExpiringStorage) cleanupLoop() {
	ticker := time.NewTicker(s.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCleanup:
			return
		case <-ticker.C:
			s.removeExpiredItems()
		}
	}
}

func (s *ExpiringStorage) removeExpiredItems() {
	s.cachesMu.RLock()
	entries := make([]*cacheEntry, 0, len(s.caches))
	for _, entry := range s.caches {
		entries = append(entries, entry)
	}
	s.cachesMu.RUnlock()

	for _, entry := range entries {
		entry.cache.RemoveExpiredEntries()
	}
}
