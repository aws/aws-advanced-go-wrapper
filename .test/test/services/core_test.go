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

	"github.com/aws/aws-advanced-go-wrapper/awssql/services"
	"github.com/stretchr/testify/assert"
)

func TestGetCoreServiceContainer(t *testing.T) {
	defer services.ResetForTesting()

	core := services.GetCoreServiceContainer()
	assert.NotNil(t, core)
	assert.NotNil(t, core.Storage)
	assert.NotNil(t, core.Monitor)
	assert.NotNil(t, core.Events)
}

func TestGetCoreServiceContainerSingleton(t *testing.T) {
	defer services.ResetForTesting()

	core1 := services.GetCoreServiceContainer()
	core2 := services.GetCoreServiceContainer()
	assert.Same(t, core1, core2)
}

func TestShutdown(t *testing.T) {
	core := services.GetCoreServiceContainer()
	assert.NotNil(t, core)

	services.Shutdown()

	// After shutdown, getting the container again should create a new one
	core2 := services.GetCoreServiceContainer()
	assert.NotNil(t, core2)
	assert.NotSame(t, core, core2)

	services.ResetForTesting()
}

func TestResetForTesting(t *testing.T) {
	core := services.GetCoreServiceContainer()
	assert.NotNil(t, core)

	services.ResetForTesting()

	// After reset, getting the container again should create a new one
	core2 := services.GetCoreServiceContainer()
	assert.NotNil(t, core2)
	assert.NotSame(t, core, core2)

	services.ResetForTesting()
}

func TestShutdownWhenNil(t *testing.T) {
	services.ResetForTesting()
	// Should not panic when called without initialization
	services.Shutdown()
}

func TestDefaultConstants(t *testing.T) {
	assert.Greater(t, services.DefaultStorageCleanupInterval.Seconds(), float64(0))
	assert.Greater(t, services.DefaultMonitorCheckInterval.Seconds(), float64(0))
}
