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

package bun_pg_driver

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/uptrace/bun/driver/pgdriver"
)

type BunPgUnderlyingDriver struct {
	inner pgdriver.Driver
}

// NewUnderlyingDriver returns the pgdriver instance the wrapper should use.
func NewUnderlyingDriver() BunPgUnderlyingDriver {
	return BunPgUnderlyingDriver{inner: pgdriver.NewDriver()}
}

var _ driver.Driver = BunPgUnderlyingDriver{}
var _ driver.DriverContext = BunPgUnderlyingDriver{}

func (d BunPgUnderlyingDriver) Open(name string) (driver.Conn, error) {
	return wrapConn(d.inner.Open(name))
}

func (d BunPgUnderlyingDriver) OpenConnector(name string) (driver.Connector, error) {
	connector, err := d.inner.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return bunPgConnector{inner: connector, driver: d}, nil
}

type bunPgConnector struct {
	inner  driver.Connector
	driver driver.Driver
}

func (c bunPgConnector) Connect(ctx context.Context) (driver.Conn, error) {
	return wrapConn(c.inner.Connect(ctx))
}

func (c bunPgConnector) Driver() driver.Driver {
	return c.driver
}

func wrapConn(conn driver.Conn, err error) (driver.Conn, error) {
	if err != nil {
		return nil, err
	}
	pgConn, ok := conn.(*pgdriver.Conn)
	if !ok {
		return conn, nil
	}
	return &bunPgConn{Conn: pgConn}, nil
}

type bunPgConn struct {
	*pgdriver.Conn
}

var _ driver.NamedValueChecker = (*bunPgConn)(nil)

func (c *bunPgConn) CheckNamedValue(namedValue *driver.NamedValue) error {
	if namedValue.Name != "" {
		return fmt.Errorf("bun-pg-driver: named parameter %q is not supported, use ordinal $N placeholders", namedValue.Name)
	}

	value, err := driver.DefaultParameterConverter.ConvertValue(namedValue.Value)
	if err != nil {
		return err
	}
	namedValue.Value = value
	return nil
}
