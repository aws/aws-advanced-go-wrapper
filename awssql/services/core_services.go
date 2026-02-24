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
)

const (
	DefaultStorageCleanupInterval = 30 * time.Second
	DefaultMonitorCheckInterval   = 10 * time.Second
)

var (
	coreServiceContainer     *CoreServiceContainer
	coreServiceContainerOnce sync.Once
)

// CoreServiceContainer holds the shared singleton services used across all connections.
type CoreServiceContainer struct {
	Storage driver_infrastructure.StorageService
	Monitor driver_infrastructure.MonitorService
	Events  driver_infrastructure.EventPublisher
}

// GetCoreServiceContainer returns the shared singleton core services, initializing on first call.
func GetCoreServiceContainer() *CoreServiceContainer {
	coreServiceContainerOnce.Do(func() {
		events := NewBatchingEventPublisher(DefaultMessageInterval)
		coreServiceContainer = &CoreServiceContainer{
			Storage: NewExpiringStorage(DefaultStorageCleanupInterval, events),
			Monitor: NewMonitorManager(DefaultMonitorCheckInterval, events),
			Events:  events,
		}
	})
	return coreServiceContainer
}

// Shutdown stops all background goroutines in the core services.
// Call this on program exit to clean up resources.
func Shutdown() {
	if coreServiceContainer != nil {
		coreServiceContainer.Storage.Stop()
		coreServiceContainer.Monitor.ReleaseResources()
		coreServiceContainer.Events.Stop()
	}
}

// ResetForTesting resets the singleton for testing purposes.
// This should only be used in tests.
func ResetForTesting() {
	if coreServiceContainer != nil {
		coreServiceContainer.Storage.Stop()
		coreServiceContainer.Monitor.ReleaseResources()
		coreServiceContainer.Events.Stop()
	}
	coreServiceContainer = nil
	coreServiceContainerOnce = sync.Once{}
}
