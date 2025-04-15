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

package driver_infrastructure

import (
	"awssql/error_util"
	"awssql/property_util"
	"awssql/utils"
	"database/sql/driver"
	"reflect"
)

type DriverDialectProvider interface {
	GetDialect(driver driver.Driver, props map[string]string) DriverDialect
	RegisterDriver(props map[string]string, drivers map[string]driver.Driver) bool
}
type DriverDialectManager struct{}

const (
	PGX_DRIVER_DIALECT_CODE   = "pgx"
	MYSQL_DRIVER_DIALECT_CODE = "mysql"
)

var knownDriverDialectsByCode = map[string]DriverDialect{
	PGX_DRIVER_DIALECT_CODE:   NewPgxDriverDialect(),
	MYSQL_DRIVER_DIALECT_CODE: NewMySQLDriverDialect(),
}

var defaultDriverDialectsByProtocol = map[string]DriverDialect{
	utils.PGX_DRIVER_PROTOCOL:   NewPgxDriverDialect(),
	utils.MYSQL_DRIVER_PROTOCOL: NewMySQLDriverDialect(),
}

func (ddm DriverDialectManager) GetDialect(driver driver.Driver, props map[string]string) (DriverDialect, error) {
	dialectCode := property_util.TARGET_DRIVER_DIALECT.Get(props)
	if dialectCode != "" {
		driverDialect, ok := knownDriverDialectsByCode[dialectCode]
		if !ok {
			return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("DriverDialectManager.unknownDriverDialectCode", dialectCode))
		}
		return driverDialect, nil
	}

	for _, candidateDriverDialect := range knownDriverDialectsByCode {
		if candidateDriverDialect.IsDialect(driver) {
			return candidateDriverDialect, nil
		}
	}

	return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("DriverDialectManager.unableToGetDialect", reflect.TypeOf(driver).String()))
}

func (ddm DriverDialectManager) RegisterDriver(props map[string]string, drivers map[string]driver.Driver) (bool, error) {
	driverAutoRegister, err := property_util.GetVerifiedWrapperPropertyValue[bool](props, property_util.TARGET_DRIVER_AUTO_REGISTER)
	if err == nil && !driverAutoRegister {
		return false, nil
	}

	var driverDialect DriverDialect
	dialectCode := property_util.TARGET_DRIVER_DIALECT.Get(props)
	if dialectCode != "" {
		driverDialect = knownDriverDialectsByCode[dialectCode]
		if driverDialect == nil {
			return false, error_util.NewGenericAwsWrapperError(error_util.GetMessage("DriverDialectManager.unknownDriverDialectCode", dialectCode))
		}
	}

	if driverDialect == nil {
		protocol := property_util.DRIVER_PROTOCOL.Get(props)
		driverDialect = defaultDriverDialectsByProtocol[protocol]
		if driverDialect == nil {
			return false, error_util.NewGenericAwsWrapperError(error_util.GetMessage("DriverDialectManager.unknownProtocol", protocol))
		}
	}

	if !driverDialect.IsDriverRegistered(drivers) {
		driverDialect.RegisterDriver()
	}

	return true, nil
}
