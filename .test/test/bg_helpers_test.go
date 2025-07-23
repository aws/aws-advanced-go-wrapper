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

package test

import (
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/bg"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/stretchr/testify/assert"
)

func TestBlueGreenPhase(t *testing.T) {
	tests := []struct {
		name             string
		phase            driver_infrastructure.BlueGreenPhase
		expectedName     string
		expectedPhase    int
		expectedIsActive bool
	}{
		{
			name:             "NotCreated",
			phase:            driver_infrastructure.NOT_CREATED,
			expectedName:     "NOT_CREATED",
			expectedPhase:    0,
			expectedIsActive: false,
		},
		{
			name:             "Created",
			phase:            driver_infrastructure.CREATED,
			expectedName:     "CREATED",
			expectedPhase:    1,
			expectedIsActive: false,
		},
		{
			name:             "Preparation",
			phase:            driver_infrastructure.PREPARATION,
			expectedName:     "PREPARATION",
			expectedPhase:    2,
			expectedIsActive: true,
		},
		{
			name:             "InProgress",
			phase:            driver_infrastructure.IN_PROGRESS,
			expectedName:     "IN_PROGRESS",
			expectedPhase:    3,
			expectedIsActive: true,
		},
		{
			name:             "Post",
			phase:            driver_infrastructure.POST,
			expectedName:     "POST",
			expectedPhase:    4,
			expectedIsActive: true,
		},
		{
			name:             "Completed",
			phase:            driver_infrastructure.COMPLETED,
			expectedName:     "COMPLETED",
			expectedPhase:    5,
			expectedIsActive: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name := tt.phase.GetName()
			assert.Equal(t, tt.expectedName, name)
			phaseInt := tt.phase.GetPhase()
			assert.Equal(t, tt.expectedPhase, phaseInt)
			isActiveSwitchoverOrCompleted := tt.phase.IsActiveSwitchoverOrCompleted()
			assert.Equal(t, tt.expectedIsActive, isActiveSwitchoverOrCompleted)
		})
	}
}

func TestBlueGreenPhase_IsZero(t *testing.T) {
	tests := []struct {
		name     string
		phase    driver_infrastructure.BlueGreenPhase
		expected bool
	}{
		{
			name:     "NotCreated",
			phase:    driver_infrastructure.NOT_CREATED,
			expected: false,
		},
		{
			name:     "Created",
			phase:    driver_infrastructure.CREATED,
			expected: false,
		},
		{
			name:     "EmptyPhase",
			phase:    driver_infrastructure.BlueGreenPhase{},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.phase.IsZero()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBlueGreenPhase_Equals(t *testing.T) {
	tests := []struct {
		name     string
		phase1   driver_infrastructure.BlueGreenPhase
		phase2   driver_infrastructure.BlueGreenPhase
		expected bool
	}{
		{
			name:     "SamePhases",
			phase1:   driver_infrastructure.CREATED,
			phase2:   driver_infrastructure.CREATED,
			expected: true,
		},
		{
			name:     "DifferentPhases",
			phase1:   driver_infrastructure.CREATED,
			phase2:   driver_infrastructure.PREPARATION,
			expected: false,
		},
		{
			name:     "ZeroPhases",
			phase1:   driver_infrastructure.BlueGreenPhase{},
			phase2:   driver_infrastructure.BlueGreenPhase{},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.phase1.Equals(tt.phase2)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBlueGreenParsePhase(t *testing.T) {
	tests := []struct {
		name      string
		statusKey string
		expected  driver_infrastructure.BlueGreenPhase
	}{
		{
			name:      "EmptyString",
			statusKey: "",
			expected:  driver_infrastructure.NOT_CREATED,
		},
		{
			name:      "Available",
			statusKey: "AVAILABLE",
			expected:  driver_infrastructure.CREATED,
		},
		{
			name:      "SwitchoverInitiated",
			statusKey: "SWITCHOVER_INITIATED",
			expected:  driver_infrastructure.PREPARATION,
		},
		{
			name:      "SwitchoverInProgress",
			statusKey: "SWITCHOVER_IN_PROGRESS",
			expected:  driver_infrastructure.IN_PROGRESS,
		},
		{
			name:      "SwitchoverInPostProcessing",
			statusKey: "SWITCHOVER_IN_POST_PROCESSING",
			expected:  driver_infrastructure.POST,
		},
		{
			name:      "SwitchoverCompleted",
			statusKey: "SWITCHOVER_COMPLETED",
			expected:  driver_infrastructure.COMPLETED,
		},
		{
			name:      "LowercaseStatus",
			statusKey: "available",
			expected:  driver_infrastructure.CREATED,
		},
		{
			name:      "MixedCaseStatus",
			statusKey: "Switchover_Initiated",
			expected:  driver_infrastructure.PREPARATION,
		},
		{
			name:      "UnknownStatus",
			statusKey: "UNKNOWN_STATUS",
			expected:  driver_infrastructure.BlueGreenPhase{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := driver_infrastructure.ParsePhase(tt.statusKey)
			if tt.statusKey == "UNKNOWN_STATUS" {
				assert.True(t, result.IsZero())
			} else {
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestBlueGreenRole_GetName(t *testing.T) {
	tests := []struct {
		name     string
		role     driver_infrastructure.BlueGreenRole
		expected string
	}{
		{
			name:     "SourceRole",
			role:     driver_infrastructure.SOURCE,
			expected: "SOURCE",
		},
		{
			name:     "TargetRole",
			role:     driver_infrastructure.TARGET,
			expected: "TARGET",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.role.GetName()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBlueGreenRole_GetValue(t *testing.T) {
	tests := []struct {
		name     string
		role     driver_infrastructure.BlueGreenRole
		expected int
	}{
		{
			name:     "SourceRole",
			role:     driver_infrastructure.SOURCE,
			expected: 0,
		},
		{
			name:     "TargetRole",
			role:     driver_infrastructure.TARGET,
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.role.GetValue()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBlueGreenRole_IsZero(t *testing.T) {
	tests := []struct {
		name     string
		role     driver_infrastructure.BlueGreenRole
		expected bool
	}{
		{
			name:     "SourceRole",
			role:     driver_infrastructure.SOURCE,
			expected: false,
		},
		{
			name:     "TargetRole",
			role:     driver_infrastructure.TARGET,
			expected: false,
		},
		{
			name:     "EmptyRole",
			role:     driver_infrastructure.BlueGreenRole{},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.role.IsZero()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBlueGreenRole_String(t *testing.T) {
	tests := []struct {
		name     string
		role     driver_infrastructure.BlueGreenRole
		expected string
	}{
		{
			name:     "Source",
			role:     driver_infrastructure.SOURCE,
			expected: "BlueGreenRole [name: SOURCE, value: 0]",
		},
		{
			name:     "Target",
			role:     driver_infrastructure.TARGET,
			expected: "BlueGreenRole [name: TARGET, value: 1]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.role.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBlueGreenParseRole(t *testing.T) {
	tests := []struct {
		name     string
		roleKey  string
		expected driver_infrastructure.BlueGreenRole
	}{
		{
			name:     "Source",
			roleKey:  "BLUE_GREEN_DEPLOYMENT_SOURCE",
			expected: driver_infrastructure.SOURCE,
		},
		{
			name:     "Target",
			roleKey:  "BLUE_GREEN_DEPLOYMENT_TARGET",
			expected: driver_infrastructure.TARGET,
		},
		{
			name:     "LowercaseRole",
			roleKey:  "blue_green_deployment_source",
			expected: driver_infrastructure.SOURCE,
		},
		{
			name:     "MixedCaseRole",
			roleKey:  "Blue_Green_Deployment_Target",
			expected: driver_infrastructure.TARGET,
		},
		{
			name:     "UnknownRole",
			roleKey:  "UNKNOWN_ROLE",
			expected: driver_infrastructure.BlueGreenRole{},
		},
		{
			name:     "EmptyRole",
			roleKey:  "",
			expected: driver_infrastructure.BlueGreenRole{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := driver_infrastructure.ParseRole(tt.roleKey)
			if tt.roleKey == "UNKNOWN_ROLE" || tt.roleKey == "" {
				assert.True(t, result.IsZero())
			} else {
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}
func TestNewBgStatus(t *testing.T) {
	id := "test-bg-id"
	phase := driver_infrastructure.CREATED
	var connectRouting []driver_infrastructure.ConnectRouting
	var executeRouting []driver_infrastructure.ExecuteRouting
	roleByHost := utils.NewRWMap[driver_infrastructure.BlueGreenRole]()
	correspondingHosts := utils.NewRWMap[utils.Pair[*host_info_util.HostInfo, *host_info_util.HostInfo]]()

	status := driver_infrastructure.NewBgStatus(id, phase, connectRouting, executeRouting, roleByHost, correspondingHosts)
	assert.Equal(t, phase, status.GetCurrentPhase())
	assert.Equal(t, connectRouting, status.GetConnectRouting())
	assert.Equal(t, executeRouting, status.GetExecuteRouting())
	assert.NotNil(t, status.GetCorrespondingHosts())
}

func TestBlueGreenStatus_GetRole(t *testing.T) {
	roleByHost := utils.NewRWMap[driver_infrastructure.BlueGreenRole]()

	host := &host_info_util.HostInfo{Host: "test.example.com", Port: 5432}
	roleByHost.Put("test.example.com", driver_infrastructure.SOURCE)

	status := driver_infrastructure.NewBgStatus(
		"test-id",
		driver_infrastructure.CREATED,
		[]driver_infrastructure.ConnectRouting{},
		[]driver_infrastructure.ExecuteRouting{},
		roleByHost,
		utils.NewRWMap[utils.Pair[*host_info_util.HostInfo, *host_info_util.HostInfo]](),
	)

	tests := []struct {
		name         string
		hostInfo     *host_info_util.HostInfo
		expectedRole driver_infrastructure.BlueGreenRole
		expectedOk   bool
	}{
		{
			name:         "ExistingHost",
			hostInfo:     host,
			expectedRole: driver_infrastructure.SOURCE,
			expectedOk:   true,
		},
		{
			name:         "Non-existingHost",
			hostInfo:     &host_info_util.HostInfo{Host: "nonexistent.example.com", Port: 5432},
			expectedRole: driver_infrastructure.BlueGreenRole{},
			expectedOk:   false,
		},
		{
			name:         "NilHost",
			hostInfo:     nil,
			expectedRole: driver_infrastructure.BlueGreenRole{},
			expectedOk:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			role, ok := status.GetRole(tt.hostInfo)
			assert.Equal(t, tt.expectedOk, ok)
			if tt.expectedOk {
				assert.Equal(t, tt.expectedRole, role)
			}
		})
	}
}

func TestBlueGreenStatus_IsZero(t *testing.T) {
	tests := []struct {
		name     string
		status   driver_infrastructure.BlueGreenStatus
		expected bool
	}{
		{
			name:     "Zero",
			status:   driver_infrastructure.BlueGreenStatus{},
			expected: true,
		},
		{
			name: "NonZeroId",
			status: driver_infrastructure.NewBgStatus(
				"test-id",
				driver_infrastructure.BlueGreenPhase{},
				nil,
				nil,
				nil,
				nil,
			),
			expected: false,
		},
		{
			name: "NonZeroPhase",
			status: driver_infrastructure.NewBgStatus(
				"",
				driver_infrastructure.CREATED,
				nil,
				nil,
				nil,
				nil,
			),
			expected: false,
		},
		{
			name: "NonZeroRouting",
			status: driver_infrastructure.NewBgStatus(
				"",
				driver_infrastructure.BlueGreenPhase{},
				[]driver_infrastructure.ConnectRouting{},
				nil,
				nil,
				nil,
			),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.status.IsZero()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBlueGreenStatus_MatchIdPhaseAndLen(t *testing.T) {
	baseStatus := driver_infrastructure.NewBgStatus(
		"test-id",
		driver_infrastructure.CREATED,
		[]driver_infrastructure.ConnectRouting{},
		[]driver_infrastructure.ExecuteRouting{},
		utils.NewRWMap[driver_infrastructure.BlueGreenRole](),
		utils.NewRWMap[utils.Pair[*host_info_util.HostInfo, *host_info_util.HostInfo]](),
	)

	tests := []struct {
		name     string
		other    driver_infrastructure.BlueGreenStatus
		expected bool
	}{
		{
			name: "IdenticalStatus",
			other: driver_infrastructure.NewBgStatus(
				"test-id",
				driver_infrastructure.CREATED,
				[]driver_infrastructure.ConnectRouting{},
				[]driver_infrastructure.ExecuteRouting{},
				utils.NewRWMap[driver_infrastructure.BlueGreenRole](),
				utils.NewRWMap[utils.Pair[*host_info_util.HostInfo, *host_info_util.HostInfo]](),
			),
			expected: true,
		},
		{
			name: "DifferentID",
			other: driver_infrastructure.NewBgStatus(
				"different-id",
				driver_infrastructure.CREATED,
				[]driver_infrastructure.ConnectRouting{},
				[]driver_infrastructure.ExecuteRouting{},
				utils.NewRWMap[driver_infrastructure.BlueGreenRole](),
				utils.NewRWMap[utils.Pair[*host_info_util.HostInfo, *host_info_util.HostInfo]](),
			),
			expected: false,
		},
		{
			name: "DifferentPhase",
			other: driver_infrastructure.NewBgStatus(
				"test-id",
				driver_infrastructure.PREPARATION,
				[]driver_infrastructure.ConnectRouting{},
				[]driver_infrastructure.ExecuteRouting{},
				utils.NewRWMap[driver_infrastructure.BlueGreenRole](),
				utils.NewRWMap[utils.Pair[*host_info_util.HostInfo, *host_info_util.HostInfo]](),
			),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := baseStatus.MatchIdPhaseAndLen(tt.other)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBlueGreenStatus_String(t *testing.T) {
	status := driver_infrastructure.NewBgStatus(
		"test-bg-id",
		driver_infrastructure.CREATED,
		[]driver_infrastructure.ConnectRouting{},
		[]driver_infrastructure.ExecuteRouting{},
		utils.NewRWMap[driver_infrastructure.BlueGreenRole](),
		utils.NewRWMap[utils.Pair[*host_info_util.HostInfo, *host_info_util.HostInfo]](),
	)

	result := status.String()

	assert.Contains(t, result, "BlueGreenStatus")
	assert.Contains(t, result, "test-bg-id")
	assert.Contains(t, result, "CREATED")
	assert.Contains(t, result, "connect routing")
	assert.Contains(t, result, "execute routing")
	assert.Contains(t, result, "roleByHost")
}

func TestBlueGreenResult_String(t *testing.T) {
	result := &driver_infrastructure.BlueGreenResult{
		Version:  "1.0",
		Endpoint: "test.example.com",
		Port:     5432,
		Role:     "SOURCE",
		Status:   "AVAILABLE",
	}

	stringResult := result.String()

	assert.Contains(t, stringResult, "BlueGreenResult")
	assert.Contains(t, stringResult, "1.0")
	assert.Contains(t, stringResult, "test.example.com")
	assert.Contains(t, stringResult, "5432")
	assert.Contains(t, stringResult, "SOURCE")
	assert.Contains(t, stringResult, "AVAILABLE")
}

func TestBlueGreenRoutingResultHolder_GetResult(t *testing.T) {
	tests := []struct {
		name           string
		holder         driver_infrastructure.RoutingResultHolder
		expectedValue1 any
		expectedValue2 any
		expectedOk     bool
		expectedErr    error
	}{
		{
			name: "ReturnsAllWrappedValues",
			holder: driver_infrastructure.RoutingResultHolder{
				WrappedReturnValue:  "test-value",
				WrappedReturnValue2: 42,
				WrappedOk:           true,
				WrappedErr:          nil,
			},
			expectedValue1: "test-value",
			expectedValue2: 42,
			expectedOk:     true,
			expectedErr:    nil,
		},
		{
			name: "ReturnsError",
			holder: driver_infrastructure.RoutingResultHolder{
				WrappedReturnValue:  nil,
				WrappedReturnValue2: nil,
				WrappedOk:           false,
				WrappedErr:          assert.AnError,
			},
			expectedValue1: nil,
			expectedValue2: nil,
			expectedOk:     false,
			expectedErr:    assert.AnError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value1, value2, ok, err := tt.holder.GetResult()
			assert.Equal(t, tt.expectedValue1, value1)
			assert.Equal(t, tt.expectedValue2, value2)
			assert.Equal(t, tt.expectedOk, ok)
			assert.Equal(t, tt.expectedErr, err)
		})
	}
}

func TestBlueGreenRoutingResultHolder_IsPresent(t *testing.T) {
	tests := []struct {
		name     string
		holder   driver_infrastructure.RoutingResultHolder
		expected bool
	}{
		{
			name:     "Empty",
			holder:   driver_infrastructure.EMPTY_ROUTING_RESULT_HOLDER,
			expected: false,
		},
		{
			name: "NonEmpty",
			holder: driver_infrastructure.RoutingResultHolder{
				WrappedReturnValue: "test-value",
			},
			expected: true,
		},
		{
			name: "Error",
			holder: driver_infrastructure.RoutingResultHolder{
				WrappedErr: assert.AnError,
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.holder.IsPresent()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBlueGreenConstants(t *testing.T) {
	assert.Equal(t, driver_infrastructure.BlueGreenIntervalRate(0), driver_infrastructure.BASELINE)
	assert.Equal(t, driver_infrastructure.BlueGreenIntervalRate(1), driver_infrastructure.INCREASED)
	assert.Equal(t, driver_infrastructure.BlueGreenIntervalRate(2), driver_infrastructure.HIGH)

	assert.Equal(t, "BLUE_GREEN_DEPLOYMENT_SOURCE", driver_infrastructure.BLUE_GREEN_SOURCE)
	assert.Equal(t, "BLUE_GREEN_DEPLOYMENT_TARGET", driver_infrastructure.BLUE_GREEN_TARGET)

	assert.Equal(t, "AVAILABLE", driver_infrastructure.AVAILABLE)
	assert.Equal(t, "SWITCHOVER_INITIATED", driver_infrastructure.SWITCHOVER_INITIATED)
	assert.Equal(t, "SWITCHOVER_IN_PROGRESS", driver_infrastructure.SWITCHOVER_IN_PROGRESS)
	assert.Equal(t, "SWITCHOVER_IN_POST_PROCESSING", driver_infrastructure.SWITCHOVER_IN_POST_PROCESSING)
	assert.Equal(t, "SWITCHOVER_COMPLETED", driver_infrastructure.SWITCHOVER_COMPLETED)
}

func TestBlueGreenInterimStatus_IsZero(t *testing.T) {
	var nilStatus *bg.BlueGreenInterimStatus
	assert.True(t, nilStatus.IsZero(), "Nil status should be zero")

	zeroStatus := &bg.BlueGreenInterimStatus{}
	assert.True(t, zeroStatus.IsZero(), "Empty status should be zero")

	nonZeroStatus := &bg.BlueGreenInterimStatus{
		Version: "1.0",
	}
	assert.False(t, nonZeroStatus.IsZero(), "Status with version should not be zero")
}

func TestBlueGreenInterimStatus_String(t *testing.T) {
	status := &bg.BlueGreenInterimStatus{
		Phase:   driver_infrastructure.CREATED,
		Version: "1.0",
		Port:    5432,
	}

	result := status.String()
	assert.Contains(t, result, "BlueGreenInterimStatus", "String should contain type name")
	assert.Contains(t, result, "CREATED", "String should contain phase")
	assert.Contains(t, result, "1.0", "String should contain version")
	assert.Contains(t, result, "5432", "String should contain port")
}

func TestBlueGreenInterimStatus_GetCustomHashCode(t *testing.T) {
	status1 := &bg.BlueGreenInterimStatus{
		Phase:   driver_infrastructure.CREATED,
		Version: "1.0",
		Port:    5432,
	}

	status2 := &bg.BlueGreenInterimStatus{
		Phase:   driver_infrastructure.CREATED,
		Version: "1.0",
		Port:    5432,
	}

	status3 := &bg.BlueGreenInterimStatus{
		Phase:   driver_infrastructure.PREPARATION,
		Version: "1.0",
		Port:    5432,
	}

	// Same content should produce same hash
	hash1 := status1.GetCustomHashCode()
	hash2 := status2.GetCustomHashCode()
	hash3 := status3.GetCustomHashCode()
	assert.Equal(t, hash1, hash2, "Same content should produce same hash")
	assert.NotEqual(t, hash1, hash3, "Different content should produce different hash")
	assert.NotEqual(t, hash2, hash3, "Different content should produce different hash")
}

func TestStatusInfo_IsZero(t *testing.T) {
	var nilStatus *bg.StatusInfo
	assert.True(t, nilStatus.IsZero(), "Nil StatusInfo should be zero")

	zeroStatus := &bg.StatusInfo{}
	assert.True(t, zeroStatus.IsZero(), "Empty StatusInfo should be zero")

	nonZeroStatus := &bg.StatusInfo{
		Version: "1.0",
	}
	assert.False(t, nonZeroStatus.IsZero(), "StatusInfo with version should not be zero")
}
