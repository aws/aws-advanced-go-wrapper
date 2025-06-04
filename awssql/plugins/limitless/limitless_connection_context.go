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

package limitless

import (
	"database/sql/driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
)

type LimitlessConnectionContext struct {
	Host             host_info_util.HostInfo
	Props            map[string]string
	connection       driver.Conn
	ConnectFunc      driver_infrastructure.ConnectFunc
	LimitlessRouters []*host_info_util.HostInfo
}

func NewConnectionContext(
	hostInfo host_info_util.HostInfo,
	props map[string]string,
	conn driver.Conn,
	connectFunc driver_infrastructure.ConnectFunc,
	limitlessRouters []*host_info_util.HostInfo) *LimitlessConnectionContext {
	return &LimitlessConnectionContext{
		Host:             hostInfo,
		Props:            props,
		connection:       conn,
		ConnectFunc:      connectFunc,
		LimitlessRouters: limitlessRouters,
	}
}

func (connectionContext *LimitlessConnectionContext) SetConnection(connection driver.Conn) {
	if connectionContext.connection != nil && connectionContext.connection != connection {
		_ = connectionContext.connection.Close()
	}
	connectionContext.connection = connection
}

func (connectionContext *LimitlessConnectionContext) GetConnection() driver.Conn {
	return connectionContext.connection
}
