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

package internal_pool

import (
	"fmt"
)

type PoolKey struct {
	url      string
	extraKey string
}

func NewPoolKey(url string, extraKey string) *PoolKey {
	return &PoolKey{
		url:      url,
		extraKey: extraKey,
	}
}

func (pk *PoolKey) GetUrl() string {
	return pk.url
}

func (pk *PoolKey) GetExtraKey() string {
	return pk.extraKey
}

func (pk *PoolKey) GetPoolKeyString() string {
	return fmt.Sprint("PoolKey [url=", pk.url, ", extraKey=", pk.extraKey, "]")
}
