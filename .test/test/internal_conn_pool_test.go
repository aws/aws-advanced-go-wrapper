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
	"context"
	"database/sql/driver"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	mock_database_sql_driver "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/database_sql_driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/internal_pool"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

type mockPoolConn struct {
	driver.Conn
}

func (c *mockPoolConn) ResetSession(_ context.Context) error {
	return nil
}

func TestConnPool_GetAndPut_Basic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockConn.EXPECT().Close().Times(1)

	pool := internal_pool.NewConnPool(func() (driver.Conn, error) {
		return &mockPoolConn{mockConn}, nil
	}, internal_pool.NewInternalPoolOptions(
		internal_pool.WithMaxIdleConns(1),
		internal_pool.WithMaxConnLifetime(time.Minute),
		internal_pool.WithMaxConnIdleTime(time.Minute),
	))

	conn, err := pool.Get()
	assert.NoError(t, err)

	err = conn.Close() // Use Close() instead of Put()
	assert.NoError(t, err)

	err = pool.Close()
	assert.NoError(t, err)
}

func TestConnPool_ReusesIdle(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var created int32
	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockConn.EXPECT().Close().Times(1)

	pool := internal_pool.NewConnPool(func() (driver.Conn, error) {
		atomic.AddInt32(&created, 1)
		return &mockPoolConn{mockConn}, nil
	}, internal_pool.NewInternalPoolOptions(
		internal_pool.WithMaxIdleConns(1),
		internal_pool.WithMaxConnLifetime(time.Minute),
		internal_pool.WithMaxConnIdleTime(time.Minute),
	))

	conn1, err := pool.Get()
	assert.NoError(t, err)

	err = conn1.Close()
	assert.NoError(t, err)

	conn2, err := pool.Get()
	assert.NoError(t, err)

	err = conn2.Close()
	assert.NoError(t, err)

	err = pool.Close()
	assert.NoError(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(&created))
}

func TestConnPool_ExpiredConnection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn1 := mock_database_sql_driver.NewMockConn(ctrl)
	mockConn2 := mock_database_sql_driver.NewMockConn(ctrl)

	mockConn1.EXPECT().Close().Times(1)
	mockConn2.EXPECT().Close().Times(1)

	var callCount int32
	pool := internal_pool.NewConnPool(func() (driver.Conn, error) {
		if atomic.AddInt32(&callCount, 1) == 1 {
			return &mockPoolConn{mockConn1}, nil
		}
		return &mockPoolConn{mockConn2}, nil
	}, internal_pool.NewInternalPoolOptions(
		internal_pool.WithMaxIdleConns(1),
		internal_pool.WithMaxConnLifetime(1*time.Nanosecond),
		internal_pool.WithMaxConnIdleTime(time.Minute),
	))

	conn1, err := pool.Get()
	assert.NoError(t, err)

	err = conn1.Close()
	assert.NoError(t, err)

	// Wait for connection to expire
	time.Sleep(2 * time.Nanosecond)

	conn2, err := pool.Get()
	assert.NoError(t, err)

	err = conn2.Close()
	assert.NoError(t, err)

	err = pool.Close()
	assert.NoError(t, err)
}

func TestConnPool_UnlimitedIdle(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockConn.EXPECT().Close().Times(1)

	pool := internal_pool.NewConnPool(func() (driver.Conn, error) {
		return &mockPoolConn{mockConn}, nil
	}, internal_pool.NewInternalPoolOptions(
		internal_pool.WithMaxIdleConns(0),
		internal_pool.WithMaxConnLifetime(0),
		internal_pool.WithMaxConnIdleTime(0),
	))

	conn, err := pool.Get()
	assert.NoError(t, err)

	err = conn.Close()
	assert.NoError(t, err)

	_ = pool.Close()
}

func TestConnPool_Close_ClosesAllIdle(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockConn.EXPECT().Close().Times(1)

	pool := internal_pool.NewConnPool(func() (driver.Conn, error) {
		return &mockPoolConn{mockConn}, nil
	}, internal_pool.NewInternalPoolOptions(
		internal_pool.WithMaxIdleConns(1),
		internal_pool.WithMaxConnLifetime(0),
		internal_pool.WithMaxConnIdleTime(0),
	))

	conn, _ := pool.Get()
	_ = conn.Close()

	err := pool.Close()
	assert.NoError(t, err)
}

func TestConnPool_NewConnFails(t *testing.T) {
	expectedErr := errors.New("factory failed")
	pool := internal_pool.NewConnPool(func() (driver.Conn, error) {
		return nil, expectedErr
	}, internal_pool.NewInternalPoolOptions(
		internal_pool.WithMaxIdleConns(1),
		internal_pool.WithMaxConnLifetime(0),
		internal_pool.WithMaxConnIdleTime(0),
	))

	conn, err := pool.Get()
	assert.Nil(t, conn)
	assert.Equal(t, expectedErr, err)
}

func newEvictionTestPool(factory func() (driver.Conn, error)) *internal_pool.InternalConnPool {
	return internal_pool.NewConnPool(factory, internal_pool.NewInternalPoolOptions(
		internal_pool.WithMaxIdleConns(2),
		internal_pool.WithMaxConnLifetime(0),
		internal_pool.WithMaxConnIdleTime(0),
	))
}

func TestConnPool_EvictBeforeCloseClosesPhysicalConnection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	first := mock_database_sql_driver.NewMockConn(ctrl)
	second := mock_database_sql_driver.NewMockConn(ctrl)
	first.EXPECT().Close().Times(1)
	second.EXPECT().Close().Times(1)
	physical := []driver.Conn{&mockPoolConn{first}, &mockPoolConn{second}}
	pool := newEvictionTestPool(func() (driver.Conn, error) {
		next := physical[0]
		physical = physical[1:]
		return next, nil
	})

	conn, err := pool.Get()
	assert.NoError(t, err)
	assert.True(t, pool.Evict(conn))
	assert.NoError(t, conn.Close())

	// The evicted connection was not pooled, so the next Get opens a new one.
	next, err := pool.Get()
	assert.NoError(t, err)
	assert.NotSame(t, conn, next)
	assert.NoError(t, next.Close())
	assert.NoError(t, pool.Close())
}

func TestConnPool_EvictAfterCloseRemovesIdleConnection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var created int32
	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockConn.EXPECT().Close().Times(2)
	pool := newEvictionTestPool(func() (driver.Conn, error) {
		atomic.AddInt32(&created, 1)
		return &mockPoolConn{mockConn}, nil
	})

	conn, err := pool.Get()
	assert.NoError(t, err)
	assert.NoError(t, conn.Close())
	assert.True(t, pool.Evict(conn))

	next, err := pool.Get()
	assert.NoError(t, err)
	assert.Equal(t, int32(2), atomic.LoadInt32(&created))
	assert.NoError(t, next.Close())
	assert.NoError(t, pool.Close())
}

func TestConnPool_EvictIgnoresConnectionsFromOtherPools(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockConn.EXPECT().Close().Times(1)
	pool := newEvictionTestPool(func() (driver.Conn, error) { return &mockPoolConn{mockConn}, nil })
	other := newEvictionTestPool(func() (driver.Conn, error) { return nil, errors.New("unused") })

	conn, err := pool.Get()
	assert.NoError(t, err)
	assert.False(t, other.Evict(conn))
	assert.False(t, pool.Evict(mockConn))

	// The connection is still pooled and closed only when the pool closes.
	assert.NoError(t, conn.Close())
	assert.NoError(t, pool.Close())
}

func TestConnPool_ConcurrentEvictAndCloseClosePhysicalConnectionOnce(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	for i := 0; i < 200; i++ {
		var closes int32
		mockConn := mock_database_sql_driver.NewMockConn(ctrl)
		mockConn.EXPECT().Close().DoAndReturn(func() error {
			atomic.AddInt32(&closes, 1)
			return nil
		}).AnyTimes()
		pool := newEvictionTestPool(func() (driver.Conn, error) { return &mockPoolConn{mockConn}, nil })

		conn, err := pool.Get()
		assert.NoError(t, err)
		done := make(chan struct{})
		go func() {
			defer close(done)
			pool.Evict(conn)
		}()
		_ = conn.Close()
		<-done

		assert.NoError(t, pool.Close())
		assert.Equal(t, int32(1), atomic.LoadInt32(&closes))
	}
}
