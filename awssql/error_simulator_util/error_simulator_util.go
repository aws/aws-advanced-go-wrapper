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

package error_simulator_util

import (
	"database/sql"
	"database/sql/driver"

	awsdriver "github.com/aws/aws-advanced-go-wrapper/awssql/driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/error_simulator"
)

func GetErrorSimulatorFromSqlConn(conn *sql.Conn) (error_simulator.ErrorSimulator, error) {
	var errorSimulator error_simulator.ErrorSimulator
	err := conn.Raw(func(dc any) error {
		// convert to driver.conn
		driverConn, ok := dc.(driver.Conn)
		if !ok {
			return error_util.NewGenericAwsWrapperError("ErrorSimulatorHelper.cannotConvertToDriverConn")
		}

		// convert to AwsWrapperConn
		driverConnWrapper, ok := driverConn.(*awsdriver.AwsWrapperConn)
		if !ok {
			return error_util.NewGenericAwsWrapperError("ErrorSimulatorHelper.cannotConvertToAwsWrapperConn")
		}

		// get plugin instance
		plugin := driverConnWrapper.UnwrapPlugin("dev")
		if plugin == nil {
			return error_util.NewGenericAwsWrapperError("ErrorSimulatorHelper.cannotGetPluginInstance")
		}

		// get error simulator
		errorSimulator, ok = plugin.(error_simulator.ErrorSimulator)
		if !ok {
			return error_util.NewGenericAwsWrapperError("ErrorSimulatorHelper.cannotGetErrorSimulator")
		}
		return nil
	})

	return errorSimulator, err
}
