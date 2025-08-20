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

	"github.com/aws/aws-advanced-go-wrapper/awssql/internal_pool"
	"github.com/stretchr/testify/assert"
)

func TestNewInternalPoolOptions_Defaults(t *testing.T) {
	config := internal_pool.NewInternalPoolOptions()

	assert.NotNil(t, config)
	assert.Equal(t, 2, config.GetMaxIdleConns())
	assert.Equal(t, time.Duration(0), config.GetMaxConnLifetime())
	assert.Equal(t, time.Duration(0), config.GetMaxConnIdleTime())
}

func TestNewInternalPoolOptions_WithOptions(t *testing.T) {
	config := internal_pool.NewInternalPoolOptions(
		internal_pool.WithMaxIdleConns(5),
		internal_pool.WithMaxConnLifetime(time.Hour),
		internal_pool.WithMaxConnIdleTime(30*time.Minute),
	)

	assert.NotNil(t, config)
	assert.Equal(t, 5, config.GetMaxIdleConns())
	assert.Equal(t, time.Hour, config.GetMaxConnLifetime())
	assert.Equal(t, 30*time.Minute, config.GetMaxConnIdleTime())
}

func TestWithMaxIdleConns(t *testing.T) {
	config := internal_pool.NewInternalPoolOptions(
		internal_pool.WithMaxIdleConns(5),
	)

	assert.Equal(t, 5, config.GetMaxIdleConns())
}

func TestWithMaxConnLifetime(t *testing.T) {
	config := internal_pool.NewInternalPoolOptions(
		internal_pool.WithMaxConnLifetime(time.Hour),
	)

	assert.Equal(t, time.Hour, config.GetMaxConnLifetime())
}

func TestWithMaxConnIdleTime(t *testing.T) {
	config := internal_pool.NewInternalPoolOptions(
		internal_pool.WithMaxConnIdleTime(30 * time.Minute),
	)

	assert.Equal(t, 30*time.Minute, config.GetMaxConnIdleTime())
}

func TestMultipleOptions(t *testing.T) {
	config := internal_pool.NewInternalPoolOptions(
		internal_pool.WithMaxIdleConns(10),
		internal_pool.WithMaxConnLifetime(time.Hour),
	)

	assert.Equal(t, 10, config.GetMaxIdleConns())
	assert.Equal(t, time.Hour, config.GetMaxConnLifetime())
}
