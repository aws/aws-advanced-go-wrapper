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
	"log/slog"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
)

type SuspendExecuteRouting struct {
	bgId string
	BaseRouting
}

func (r *SuspendExecuteRouting) Apply(_ driver_infrastructure.ConnectionPlugin, props map[string]string,
	pluginService driver_infrastructure.PluginService, methodName string, _ driver_infrastructure.ExecuteFunc,
	_ ...any) driver_infrastructure.RoutingResultHolder {
	slog.Debug(error_util.GetMessage("BlueGreenDeployment.inProgressSuspendMethod", methodName))
	parentCtx := pluginService.GetTelemetryContext()
	telemetryFactory := pluginService.GetTelemetryFactory()
	telemetryCtx, ctx := telemetryFactory.OpenTelemetryContext(TELEMETRY_SWITCHOVER, telemetry.NESTED, parentCtx)

	pluginService.SetTelemetryContext(ctx)
	defer func() {
		telemetryCtx.CloseContext()
		pluginService.SetTelemetryContext(parentCtx)
	}()

	bgStatus, ok := pluginService.GetStatus(r.bgId)

	timeoutMs := property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.BG_CONNECT_TIMEOUT_MS)
	holdStartTime := time.Now()
	endTime := holdStartTime.Add(time.Millisecond * time.Duration(timeoutMs))

	for time.Now().Before(endTime) && ok && !bgStatus.IsZero() && bgStatus.GetCurrentPhase() == driver_infrastructure.IN_PROGRESS {
		r.Delay(SLEEP_TIME_DURATION, bgStatus, pluginService, r.bgId)
	}

	bgStatus, ok = pluginService.GetStatus(r.bgId)

	if ok && !bgStatus.IsZero() && bgStatus.GetCurrentPhase() == driver_infrastructure.IN_PROGRESS {
		return driver_infrastructure.RoutingResultHolder{WrappedErr: error_util.NewGenericAwsWrapperError(
			error_util.GetMessage("BlueGreenDeployment.inProgressTryMethodLater", timeoutMs, methodName))}
	}
	slog.Debug(error_util.GetMessage("BlueGreenDeployment.switchoverCompletedContinueWithMethod", methodName, time.Since(holdStartTime)))

	return driver_infrastructure.EMPTY_ROUTING_RESULT_HOLDER
}

func NewSuspendExecuteRouting(hostAndPort string, role driver_infrastructure.BlueGreenRole, bgId string) *SuspendExecuteRouting {
	return &SuspendExecuteRouting{bgId, NewBaseRouting(hostAndPort, role)}
}
