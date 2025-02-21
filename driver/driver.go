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

package driver

import (
	"database/sql"
	"database/sql/driver"
	"github.com/go-sql-driver/mysql"
)

type AwsWrapperDriver struct {
	targetDriver  driver.Driver
	pluginManager PluginManager
}

func (d AwsWrapperDriver) Open(dsn string) (driver.Conn, error) {
	// do wrapper driver things
	d.pluginManager.Init()

	// call underlying driver
	conn, err := d.targetDriver.Open(dsn)
	return AwsWrapperConn{underlyingConn: conn, pluginManager: d.pluginManager}, err
}

var driverName = "aws"

func init() {
	if driverName != "" {
		var targetDriver = &mysql.MySQLDriver{}
		sql.Register(
			driverName,
			&AwsWrapperDriver{
				targetDriver,
				&ConnectionPluginManager{targetDriver: targetDriver}})
	}
}

type AwsWrapperConn struct {
	underlyingConn driver.Conn
	pluginManager  PluginManager
}

func (c AwsWrapperConn) Prepare(query string) (driver.Stmt, error) {
	// TODO: complete on merge of PR #9
	panic("implement me")
}

func (c AwsWrapperConn) Close() error {
	// close wrapper driver things

	// close underlying driver
	return c.underlyingConn.Close()
}

func (c AwsWrapperConn) Begin() (driver.Tx, error) {
	// do wrapper driver things

	// call underlying driver
	return c.underlyingConn.Begin() //nolint:all
}
