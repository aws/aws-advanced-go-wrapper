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

package driver_infrastructure

import "time"

// StorageTypeDescriptor provides type-safe access to a specific type in StorageService.
// It binds a Go type (via generics) to a storage type key and configuration.
// This is similar to Java's Class<T> pattern for type-safe storage access.
type StorageTypeDescriptor[T any] struct {
	// TypeKey is the unique identifier for this storage type
	TypeKey string
	// TTL is how long items should live before expiring
	TTL time.Duration
	// RenewOnAccess controls whether accessing an item extends its expiration
	RenewOnAccess bool
	// ShouldDispose determines if an expired item should be disposed during cleanup
	ShouldDispose func(value T) bool
	// OnDispose is called when an item is removed from storage
	OnDispose func(value T)
}

// Register registers this type with the storage service.
// Must be called before using Get/Set if using custom configuration.
func (d *StorageTypeDescriptor[T]) Register(s StorageService) {
	var shouldDispose ShouldDisposeFunc
	var onDispose ItemDisposalFunc

	if d.ShouldDispose != nil {
		shouldDispose = func(v any) bool {
			return d.ShouldDispose(v.(T))
		}
	}
	if d.OnDispose != nil {
		onDispose = func(v any) {
			d.OnDispose(v.(T))
		}
	}

	s.Register(d.TypeKey, d.TTL, d.RenewOnAccess, shouldDispose, onDispose)
}

// Get retrieves a value from storage with full type safety.
// Returns the zero value and false if not found or expired.
func (d *StorageTypeDescriptor[T]) Get(s StorageService, key any) (T, bool) {
	v, ok := s.Get(d.TypeKey, key)
	if !ok || v == nil {
		var zero T
		return zero, false
	}
	result, ok := v.(T)
	if !ok {
		var zero T
		return zero, false
	}
	return result, true
}

// Set stores a value in storage with full type safety.
func (d *StorageTypeDescriptor[T]) Set(s StorageService, key any, value T) {
	s.Set(d.TypeKey, key, value)
}

// Exists checks if a non-expired value exists at the given key.
func (d *StorageTypeDescriptor[T]) Exists(s StorageService, key any) bool {
	return s.Exists(d.TypeKey, key)
}

// Remove removes a value from storage.
func (d *StorageTypeDescriptor[T]) Remove(s StorageService, key any) {
	s.Remove(d.TypeKey, key)
}

// Clear removes all values of this type from storage.
func (d *StorageTypeDescriptor[T]) Clear(s StorageService) {
	s.Clear(d.TypeKey)
}

// Size returns the number of items of this type in storage.
func (d *StorageTypeDescriptor[T]) Size(s StorageService) int {
	return s.Size(d.TypeKey)
}

// StorageRegistrar is implemented by types that can register themselves with StorageService.
type StorageRegistrar interface {
	Register(s StorageService)
}

// DefaultStorageTypes contains all built-in storage type descriptors.
// These are registered automatically when RegisterDefaultStorageTypes is called.
var DefaultStorageTypes = []StorageRegistrar{
	TopologyStorage,
	AllowedAndBlockedHostsStorage,
	// BlueGreenStatusStorage can be added here when defined
}

// RegisterDefaultStorageTypes registers all built-in storage types with the given service.
// This should be called after creating a new StorageService.
func RegisterDefaultStorageTypes(s StorageService) {
	for _, desc := range DefaultStorageTypes {
		desc.Register(s)
	}
}
