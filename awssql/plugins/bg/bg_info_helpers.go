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

package bg

import (
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
)

type StatusInfo struct {
	Version  string
	endpoint string
	port     int
	phase    driver_infrastructure.BlueGreenPhase
	role     driver_infrastructure.BlueGreenRole
}

func (s *StatusInfo) IsZero() bool {
	return s == nil || (s.Version == "" && s.endpoint == "" && s.port == 0)
}

type PhaseTimeInfo struct {
	Timestamp time.Time
	Phase     driver_infrastructure.BlueGreenPhase
}
