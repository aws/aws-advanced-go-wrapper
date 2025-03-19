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
	"awssql/error_util"
	"awssql/property_util"
	"awssql/utils"
	"database/sql/driver"
	_ "unsafe"
)

const (
	MYSQL_DRIVER_REGISTRATION_NAME  = "mysql"
	PGX_DRIVER_REGISTRATION_NAME    = "pgx"
	PGX_V5_DRIVER_REGISTRATION_NAME = "pgx/v5"
)

//go:linkname drivers database/sql.drivers
var drivers map[string]driver.Driver

// NOTE: driversMu sync.RWMutex is not linked because it is not marked in its definition. See https://go.dev/doc/go1.23#linker

func GetTargetDriver(props map[string]string) driver.Driver {
	if property_util.DRIVER_PROTOCOL.Get(props) == utils.MYSQL_DRIVER_PROTOCOL {
		mysqlDriver, ok := drivers[MYSQL_DRIVER_REGISTRATION_NAME]
		if !ok {
			panic(error_util.GetMessage("TargetDriverHelper.missingTargetDriver", "mysql"))
		}
		return mysqlDriver
	} else if property_util.DRIVER_PROTOCOL.Get(props) == utils.PGX_DRIVER_PROTOCOL {
		pgxDriver, ok := drivers[PGX_DRIVER_REGISTRATION_NAME]
		if !ok {
			pgxDriver, ok := drivers[PGX_V5_DRIVER_REGISTRATION_NAME]
			if !ok {
				panic(error_util.GetMessage("TargetDriverHelper.missingTargetDriver", "pgx or pgx/v5"))
			}
			return pgxDriver
		}
		return pgxDriver
	}
	panic(error_util.GetMessage("TargetDriverHelper.invalidProtocol", property_util.DRIVER_PROTOCOL.Get(props)))
}

func GetDatabaseEngine(props map[string]string) DatabaseEngine {
	if property_util.DRIVER_PROTOCOL.Get(props) == utils.MYSQL_DRIVER_PROTOCOL {
		return MYSQL
	} else if property_util.DRIVER_PROTOCOL.Get(props) == utils.PGX_DRIVER_PROTOCOL {
		return PG
	}
	panic(error_util.GetMessage("TargetDriverHelper.invalidProtocol", property_util.DRIVER_PROTOCOL.Get(props)))
}
