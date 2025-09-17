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

package mysql_driver_2

import (
	"database/sql"
	"database/sql/driver"

	awsDriver "github.com/aws/aws-advanced-go-wrapper/awssql/driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	_ "github.com/go-mysql-org/go-mysql/driver"
)

type MySQLDriver2 struct {
	awsWrapperDriver awsDriver.AwsWrapperDriver
}

func (d *MySQLDriver2) Open(dsn string) (driver.Conn, error) {
	d.awsWrapperDriver.DriverDialect = NewMySQL2DriverDialect()
	d.awsWrapperDriver.UnderlyingDriver = MySqlUnderlyingDriver{}
	return d.awsWrapperDriver.Open(dsn)
}

func init() {
	sql.Register(
		driver_infrastructure.AWS_MYSQL_DRIVER_CODE,
		&MySQLDriver2{})

	awsDriver.RegisterUnderlyingDriver(
		MYSQL_DRIVER_REGISTRATION_NAME,
		&MySqlUnderlyingDriver{})
}

type MySqlUnderlyingDriver struct {
}

func (d MySqlUnderlyingDriver) Open(dsn string) (driver.Conn, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}

	// Get the underlying driver connection
	return db.Driver().Open(dsn)
}
