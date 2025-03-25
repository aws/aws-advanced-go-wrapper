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
	"awssql/driver_infrastructure"
	"awssql/error_util"
	"awssql/property_util"
	"awssql/utils"
	"database/sql/driver"
	_ "unsafe"
)

//go:linkname drivers database/sql.drivers
var drivers map[string]driver.Driver

// NOTE: driversMu sync.RWMutex is not linked because it is not marked in its definition. See https://go.dev/doc/go1.23#linker

func GetTargetDriver(dsn string, props map[string]string) (driver.Driver, error) {
	targetDriver := getTargetDriverInternal(props)

	if targetDriver == nil {
		ddm := driver_infrastructure.DriverDialectManager{}
		ok, err := ddm.RegisterDriver(props, drivers)
		if ok {
			targetDriver = getTargetDriverInternal(props)
		} else if err != nil {
			return nil, err
		}
	}

	if targetDriver == nil {
		var driverNamesConcat string
		for driverName := range drivers {
			driverNamesConcat += driverName + ","
		}
		driverNamesConcat = driverNamesConcat[:len(driverNamesConcat)-1]
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("TargetDriverHelper.missingDriver", dsn, driverNamesConcat))
	}
	return targetDriver, nil
}

func getTargetDriverInternal(props map[string]string) driver.Driver {
	var targetDriver driver.Driver
	if property_util.DRIVER_PROTOCOL.Get(props) == utils.MYSQL_DRIVER_PROTOCOL {
		targetDriver = drivers[driver_infrastructure.MYSQL_DRIVER_REGISTRATION_NAME]
	} else if property_util.DRIVER_PROTOCOL.Get(props) == utils.PGX_DRIVER_PROTOCOL {
		targetDriver = drivers[driver_infrastructure.PGX_DRIVER_REGISTRATION_NAME]
		if targetDriver == nil {
			targetDriver = drivers[driver_infrastructure.PGX_V5_DRIVER_REGISTRATION_NAME]
		}
	}
	return targetDriver
}

func GetDatabaseEngine(props map[string]string) (DatabaseEngine, error) {
	if property_util.DRIVER_PROTOCOL.Get(props) == utils.MYSQL_DRIVER_PROTOCOL {
		return MYSQL, nil
	} else if property_util.DRIVER_PROTOCOL.Get(props) == utils.PGX_DRIVER_PROTOCOL {
		return PG, nil
	}
	return "", error_util.NewGenericAwsWrapperError(error_util.GetMessage("TargetDriverHelper.invalidProtocol", property_util.DRIVER_PROTOCOL.Get(props)))
}
