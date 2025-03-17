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
)

type PgxDriverDialect struct {
	errorHandler error_util.ErrorHandler
}

func NewPgxDriverDialect() *PgxDriverDialect {
	return &PgxDriverDialect{errorHandler: &PgxErrorHandler{}}
}

func (p PgxDriverDialect) GetDsnFromProperties(properties map[string]string) string {
	//TODO implement me
	panic("implement me")
}

func (p PgxDriverDialect) IsNetworkError(err error) bool {
	return p.errorHandler.IsNetworkError(err)
}

func (p PgxDriverDialect) IsLoginError(err error) bool {
	return p.errorHandler.IsLoginError(err)
}
