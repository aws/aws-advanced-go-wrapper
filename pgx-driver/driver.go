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

package pgx_driver

import (
	"database/sql"
	"database/sql/driver"
	"github.com/jackc/pgx/v5/stdlib"

	awsDriver "github.com/aws/aws-advanced-go-wrapper/awssql/driver"
)

type PgxDriver struct {
	awsWrapperDriver awsDriver.AwsWrapperDriver
}

func (d *PgxDriver) Open(dsn string) (driver.Conn, error) {
	d.awsWrapperDriver.DriverDialect = NewPgxDriverDialect()
	d.awsWrapperDriver.UnderlyingDriver = &stdlib.Driver{}
	return d.awsWrapperDriver.Open(dsn)
}

func ClearCaches() {
	awsDriver.ClearCaches()
}

func init() {
	sql.Register(
		"awssql-pgx",
		&PgxDriver{})
}
