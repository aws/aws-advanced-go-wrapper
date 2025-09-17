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
	"strings"
)

const SqlStateAccessError = "28000"

var MySql2NetworkErrorMessages = []string{
	"invalid connection",
	"bad connection",
	"broken pipe",
}

type MySQL2ErrorHandler struct {
}

func (m MySQL2ErrorHandler) IsNetworkError(err error) bool {
	for _, networkError := range MySql2NetworkErrorMessages {
		if strings.Contains(err.Error(), networkError) {
			return true
		}
	}

	return false
}

func (m MySQL2ErrorHandler) IsLoginError(err error) bool {
	return err != nil && strings.Contains(err.Error(), SqlStateAccessError)
}
