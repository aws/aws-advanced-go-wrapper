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
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

// Predefined storage type keys.
const (
	TopologyType              = "Topology"
	AllowedAndBlockedHostType = "AllowedAndBlockedHosts"
	BlueGreenStatusType       = "BlueGreenStatus"
)

// Default TTLs matching Java implementation.
const (
	defaultTopologyTTL              = 5 * time.Minute
	defaultAllowedAndBlockedHostTTL = 5 * time.Minute
	defaultBlueGreenStatusTTL       = 60 * time.Minute
)

// expirationCache holds items with expiration for a single type.
type expirationCache struct {
	items         *utils.RWMap[any, *storageItem]
	ttl           time.Duration
	renewOnAccess bool
	shouldDispose driver_infrastructure.ShouldDisposeFunc
	onDispose     driver_infrastructure.ItemDisposalFunc
}

// storageItem holds a cached value with expiration.
type storageItem struct {
	value     any
	expiresAt time.Time
}

// ExpiringStorage provides shared, expiring key-value storage for driver components.
// It maintains a map of expiration caches, with predefined caches for Topology,
// AllowedAndBlockedHosts, and BlueGreenStatus.
// Implements driver_infrastructure.StorageService.
type ExpiringStorage struct {
	caches          *utils.RWMap[string, *expirationCache]
	publisher       driver_infrastructure.EventPublisher
	cleanupInterval time.Duration
	stopCleanup     chan struct{}
}

// NewExpiringStorage creates a new expiring storage with predefined caches.
func NewExpiringStorage(cleanupInterval time.Duration, publisher driver_infrastructure.EventPublisher) *ExpiringStorage {
	s := &ExpiringStorage{
		caches:          utils.NewRWMap[string, *expirationCache](),
		publisher:       publisher,
		cleanupInterval: cleanupInterval,
		stopCleanup:     make(chan struct{}),
	}

	// Register default caches (matching Java's defaultCacheSuppliers)
	s.register(TopologyType, defaultTopologyTTL, true, nil, nil)
	s.register(AllowedAndBlockedHostType, defaultAllowedAndBlockedHostTTL, true, nil, nil)
	s.register(BlueGreenStatusType, defaultBlueGreenStatusTTL, false, nil, nil)

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
	cache := &expirationCache{
		items:         utils.NewRWMap[any, *storageItem](),
		ttl:           ttl,
		renewOnAccess: renewOnAccess,
		shouldDispose: shouldDispose,
		onDispose:     onDispose,
	}
	s.caches.PutIfAbsent(typeKey, cache)
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
	cache, ok := s.caches.Get(typeKey)
	if !ok {
		return
	}

	cache.items.Put(key, &storageItem{
		value:     value,
		expiresAt: time.Now().Add(cache.ttl),
	})
}

// Get retrieves an item by type and key. Returns nil if not found or expired.
func (s *ExpiringStorage) Get(typeKey string, key any) (any, bool) {
	cache, ok := s.caches.Get(typeKey)
	if !ok {
		return nil, false
	}

	item, ok := cache.items.Get(key)
	if !ok || item == nil {
		return nil, false
	}

	// Check if expired
	if time.Now().After(item.expiresAt) {
		cache.items.Remove(key)
		if cache.onDispose != nil {
			cache.onDispose(item.value)
		}
		return nil, false
	}

	// Renew expiration on access if configured
	if cache.renewOnAccess {
		item.expiresAt = time.Now().Add(cache.ttl)
	}

	// Publish data access event
	if s.publisher != nil {
		s.publisher.Publish(DataAccessEvent{
			TypeKey: typeKey,
			Key:     key,
		})
	}

	return item.value, true
}

// Exists checks if an item exists (and is not expired).
func (s *ExpiringStorage) Exists(typeKey string, key any) bool {
	_, ok := s.Get(typeKey, key)
	return ok
}

// Remove removes an item by type and key.
func (s *ExpiringStorage) Remove(typeKey string, key any) {
	cache, ok := s.caches.Get(typeKey)
	if !ok {
		return
	}

	item, exists := cache.items.Get(key)
	if exists && item != nil && cache.onDispose != nil {
		cache.onDispose(item.value)
	}
	cache.items.Remove(key)
}

// Clear removes all items of the given type.
func (s *ExpiringStorage) Clear(typeKey string) {
	cache, ok := s.caches.Get(typeKey)
	if !ok {
		return
	}

	if cache.onDispose != nil {
		cache.items.ForEach(func(_ any, item *storageItem) {
			cache.onDispose(item.value)
		})
	}
	cache.items.Clear()
}

// ClearAll removes all items from all caches.
func (s *ExpiringStorage) ClearAll() {
	s.caches.ForEach(func(typeKey string, cache *expirationCache) {
		if cache.onDispose != nil {
			cache.items.ForEach(func(_ any, item *storageItem) {
				cache.onDispose(item.value)
			})
		}
		cache.items.Clear()
	})
}

// Size returns the number of items stored under the given type.
func (s *ExpiringStorage) Size(typeKey string) int {
	cache, ok := s.caches.Get(typeKey)
	if !ok {
		return 0
	}
	return cache.items.Size()
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
			s.cleanup()
		}
	}
}

func (s *ExpiringStorage) cleanup() {
	now := time.Now()

	s.caches.ForEach(func(_ string, cache *expirationCache) {
		cache.items.Filter(func(_ any, item *storageItem) bool {
			// Keep if not expired
			if !now.After(item.expiresAt) {
				return true
			}
			// Item is expired - check shouldDispose
			if cache.shouldDispose != nil && !cache.shouldDispose(item.value) {
				return true // Keep it - shouldDispose said no
			}
			// Dispose and remove
			if cache.onDispose != nil {
				cache.onDispose(item.value)
			}
			return false
		})
	})
}
