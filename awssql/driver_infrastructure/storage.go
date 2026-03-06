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

// ShouldDisposeFunc determines whether an expired item should be disposed.
type ShouldDisposeFunc func(value any) bool

// ItemDisposalFunc defines how to dispose of an item when removed.
type ItemDisposalFunc func(value any)

// StorageService provides shared key-value storage for driver components.
// Access to stored data should be done through StorageTypeDescriptor for type safety.
type StorageService interface {
	Register(typeKey string, ttl time.Duration, renewOnAccess bool, shouldDispose ShouldDisposeFunc, onDispose ItemDisposalFunc)
	Exists(typeKey string, key any) bool
	Remove(typeKey string, key any)
	Clear(typeKey string)
	ClearAll()
	Size(typeKey string) int
	Stop()
}

// RawStorageAccess provides raw Get/Set methods for internal use by StorageTypeDescriptor.
// This interface should not be used directly — use StorageTypeDescriptor instead.
type RawStorageAccess interface {
	Set(typeKey string, key any, value any)
	Get(typeKey string, key any) (any, bool)
}
