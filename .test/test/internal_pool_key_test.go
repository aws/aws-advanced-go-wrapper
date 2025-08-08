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

	"github.com/aws/aws-advanced-go-wrapper/awssql/internal_pool"
	"github.com/stretchr/testify/assert"
)

func TestNewPoolKey(t *testing.T) {
	url := "test-url"
	extraKey := "test-key"

	poolKey := internal_pool.NewPoolKey(url, extraKey)

	assert.NotNil(t, poolKey)
	assert.Equal(t, url, poolKey.GetUrl())
	assert.Equal(t, extraKey, poolKey.GetExtraKey())
}

func TestPoolKey_GetUrl(t *testing.T) {
	url := "test-url"
	poolKey := internal_pool.NewPoolKey(url, "")

	assert.Equal(t, url, poolKey.GetUrl())
}

func TestPoolKey_GetExtraKey(t *testing.T) {
	extraKey := "test-key"
	poolKey := internal_pool.NewPoolKey("", extraKey)

	assert.Equal(t, extraKey, poolKey.GetExtraKey())
}

func TestPoolKey_GetPoolKeyString(t *testing.T) {
	url := "test-url"
	extraKey := "test-key"
	poolKey := internal_pool.NewPoolKey(url, extraKey)

	expected := "PoolKey [url=test-url, extraKey=test-key]"
	assert.Equal(t, expected, poolKey.GetPoolKeyString())
}

func TestPoolKey_GetPoolKeyString_EmptyValues(t *testing.T) {
	poolKey := internal_pool.NewPoolKey("", "")

	expected := "PoolKey [url=, extraKey=]"
	assert.Equal(t, expected, poolKey.GetPoolKeyString())
}
