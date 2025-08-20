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
	"context"
	"database/sql/driver"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
)

type internalPooledConn struct {
	driver.Conn
	pool       *InternalConnPool
	createdAt  time.Time
	lastUsedAt time.Time

	mu       sync.Mutex
	returned bool
}

func (pc *internalPooledConn) Close() error {
	pc.mu.Lock()
	if pc.returned {
		pc.mu.Unlock()
		return nil
	}

	pc.returned = true
	pc.mu.Unlock()
	pc.lastUsedAt = time.Now()

	pc.pool.mu.Lock()
	defer pc.pool.mu.Unlock()

	// If pool is closed, close the real conn
	if pc.pool.closed {
		return pc.Conn.Close()
	}

	// Unlimited idle
	if pc.pool.maxIdleConns == 0 || len(pc.pool.idleConns) < pc.pool.maxIdleConns {
		// We must reset the session before it is brought back to the pool
		err := pc.ResetSession(context.Background())
		if err != nil {
			slog.Warn("InternalPooledConn.CannotResetSession")
			return pc.Conn.Close()
		}
		pc.pool.idleConns = append(pc.pool.idleConns, pc)
		return nil
	}

	// Too many idle conns
	return pc.Conn.Close()
}

func (p *internalPooledConn) Ping(ctx context.Context) error {
	if pinger, ok := p.Conn.(driver.Pinger); ok {
		return pinger.Ping(ctx)
	}
	return error_util.NewGenericAwsWrapperError(
		error_util.GetMessage("InternalPooledConn.UnsupportedOperation", "driver.Pinger"))
}

func (p *internalPooledConn) ResetSession(ctx context.Context) error {
	if resetter, ok := p.Conn.(driver.SessionResetter); ok {
		return resetter.ResetSession(ctx)
	}
	return error_util.NewGenericAwsWrapperError(
		error_util.GetMessage("InternalPooledConn.UnsupportedOperation", "driver.SessionResetter"))
}

func (p *internalPooledConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if execer, ok := p.Conn.(driver.ExecerContext); ok {
		return execer.ExecContext(ctx, query, args)
	}
	return nil, error_util.NewGenericAwsWrapperError(
		error_util.GetMessage("InternalPooledConn.UnsupportedOperation", "driver.ExecerContext"))
}

func (p *internalPooledConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if queryer, ok := p.Conn.(driver.QueryerContext); ok {
		return queryer.QueryContext(ctx, query, args)
	}
	return nil, error_util.NewGenericAwsWrapperError(
		error_util.GetMessage("InternalPooledConn.UnsupportedOperation", "driver.QueryerContext"))
}

func (p *internalPooledConn) IsValid() bool {
	return !p.returned
}

type InternalConnPool struct {
	mu           sync.Mutex
	idleConns    []*internalPooledConn
	maxIdleConns int
	maxLifetime  time.Duration
	maxIdleTime  time.Duration
	newConnFunc  func() (driver.Conn, error)
	closed       bool
}

func NewConnPool(factory func() (driver.Conn, error), opts *InternalPoolConfig) *InternalConnPool {
	return &InternalConnPool{
		newConnFunc:  factory,
		maxIdleConns: opts.GetMaxIdleConns(),
		maxLifetime:  opts.GetMaxConnLifetime(),
		maxIdleTime:  opts.GetMaxConnIdleTime(),
		idleConns:    make([]*internalPooledConn, 0),
	}
}

func (p *InternalConnPool) Get() (driver.Conn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, driver.ErrBadConn
	}

	now := time.Now()
	newIdle := p.idleConns[:0]
	var pc *internalPooledConn

	for _, candidate := range p.idleConns {
		if p.isConnExpired(candidate, now) {
			_ = candidate.Conn.Close()
			continue
		}
		if pc == nil {
			pc = candidate
		} else {
			newIdle = append(newIdle, candidate)
		}
	}
	p.idleConns = newIdle

	if pc != nil {
		pc.returned = false
		return pc, nil
	}

	conn, err := p.newConnFunc()
	if err != nil {
		return nil, err
	}

	return &internalPooledConn{
		Conn:       conn,
		pool:       p,
		createdAt:  now,
		lastUsedAt: now,
	}, nil
}

func (p *InternalConnPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closed = true
	var err error
	for _, pc := range p.idleConns {
		if cerr := pc.Conn.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	p.idleConns = nil
	return err
}

func (p *InternalConnPool) isConnExpired(pc *internalPooledConn, now time.Time) bool {
	if p.maxLifetime > 0 && now.Sub(pc.createdAt) > p.maxLifetime {
		return true
	}
	if p.maxIdleTime > 0 && now.Sub(pc.lastUsedAt) > p.maxIdleTime {
		return true
	}
	return false
}
