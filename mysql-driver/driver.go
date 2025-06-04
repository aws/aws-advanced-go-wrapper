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

package mysql_driver

import (
	"database/sql"
	"database/sql/driver"
	awsDriver "github.com/aws/aws-advanced-go-wrapper/awssql/driver"
	"github.com/go-sql-driver/mysql"
)

type MySQLDriver struct {
	awsWrapperDriver awsDriver.AwsWrapperDriver
}

func (d *MySQLDriver) Open(dsn string) (driver.Conn, error) {
	d.awsWrapperDriver.DriverDialect = NewMySQLDriverDialect()
	d.awsWrapperDriver.UnderlyingDriver = &mysql.MySQLDriver{}
	return d.awsWrapperDriver.Open(dsn)
}

func (d *MySQLDriver) ReleaseResources() {
	d.awsWrapperDriver.ReleaseResources()
}

func init() {
	sql.Register(
		"awssql-mysql",
		&MySQLDriver{})
}
