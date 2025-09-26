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
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

func GetDatabaseEngine(props *utils.RWMap[string]) (driver_infrastructure.DatabaseEngine, error) {
	if property_util.DRIVER_PROTOCOL.Get(props) == property_util.MYSQL_DRIVER_PROTOCOL {
		return driver_infrastructure.MYSQL, nil
	} else if property_util.DRIVER_PROTOCOL.Get(props) == property_util.PGX_DRIVER_PROTOCOL {
		return driver_infrastructure.PG, nil
	}
	return "", error_util.NewGenericAwsWrapperError(error_util.GetMessage("TargetDriverHelper.invalidProtocol", property_util.DRIVER_PROTOCOL.Get(props)))
}
