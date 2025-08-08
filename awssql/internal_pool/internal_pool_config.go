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

import "time"

type InternalPoolConfig struct {
	maxIdleConns    int
	maxConnLifetime time.Duration
	maxConnIdleTime time.Duration
}

type InternalPoolOption func(*InternalPoolConfig)

func NewInternalPoolOptions(opts ...InternalPoolOption) *InternalPoolConfig {
	c := &InternalPoolConfig{
		maxIdleConns:    2,
		maxConnLifetime: 0,
		maxConnIdleTime: 0,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

func WithMaxIdleConns(n int) InternalPoolOption {
	return func(c *InternalPoolConfig) {
		c.maxIdleConns = n
	}
}

func WithMaxConnLifetime(d time.Duration) InternalPoolOption {
	return func(c *InternalPoolConfig) {
		c.maxConnLifetime = d
	}
}

func WithMaxConnIdleTime(d time.Duration) InternalPoolOption {
	return func(c *InternalPoolConfig) {
		c.maxConnIdleTime = d
	}
}

func (c *InternalPoolConfig) GetMaxIdleConns() int {
	return c.maxIdleConns
}

func (c *InternalPoolConfig) GetMaxConnLifetime() time.Duration {
	return c.maxConnLifetime
}

func (c *InternalPoolConfig) GetMaxConnIdleTime() time.Duration {
	return c.maxConnIdleTime
}
