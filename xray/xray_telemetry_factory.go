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

package xray

import (
	"context"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
)

func init() {
	telemetry.UseTelemetryFactory("xray", XRayTelemetryFactory{})
}

type XRayTelemetryFactory struct{}

func (X XRayTelemetryFactory) OpenTelemetryContext(name string, traceLevel telemetry.TelemetryTraceLevel, ctx context.Context) (telemetry.TelemetryContext, context.Context) {
	return NewXRayTelemetryContext(name, traceLevel, ctx)
}

func (X XRayTelemetryFactory) PostCopy(telemetryContext telemetry.TelemetryContext, traceLevel telemetry.TelemetryTraceLevel) error {
	xrayCtx, ok := telemetryContext.(*XRayTelemetryContext)
	if !ok {
		return error_util.NewGenericAwsWrapperError(error_util.GetMessage("TelemetryContext.castError", "XRayTelemetryContext"))
	}
	XRayPostCopy(*xrayCtx, traceLevel)
	return nil
}

func (X XRayTelemetryFactory) CreateCounter(name string) (telemetry.TelemetryCounter, error) {
	return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("AwsWrapper.unsupportedMethodError", "CreateCounter", "XRayTelemetryFactory"))
}

func (X XRayTelemetryFactory) CreateGauge(name string) (telemetry.TelemetryGauge, error) {
	return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("AwsWrapper.unsupportedMethodError", "CreateGauge", "XRayTelemetryFactory"))
}
