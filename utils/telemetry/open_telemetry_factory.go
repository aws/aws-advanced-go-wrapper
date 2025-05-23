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

package telemetry

import (
	"awssql/error_util"
	"context"
	"go.opentelemetry.io/otel"
)

type OpenTelemetryFactory struct{}

func (o *OpenTelemetryFactory) OpenTelemetryContext(name string, traceLevel TelemetryTraceLevel, ctx context.Context) (TelemetryContext, context.Context) {
	return NewOpenTelemetryContext(otel.GetTracerProvider().Tracer(name), name, traceLevel, ctx)
}

func (o *OpenTelemetryFactory) PostCopy(telemetryContext TelemetryContext, traceLevel TelemetryTraceLevel) error {
	otelCtx, ok := telemetryContext.(*OpenTelemetryContext)
	if !ok {
		return error_util.NewGenericAwsWrapperError(error_util.GetMessage("TelemetryContext.castError", "OpenTelemetryContext"))
	}
	OtelPostCopy(*otelCtx, traceLevel)
	return nil
}

func (o *OpenTelemetryFactory) CreateCounter(name string) (TelemetryCounter, error) {
	if name == "" {
		return nil, error_util.NewIllegalArgumentError(
			error_util.GetMessage(
				"AwsWrapper.illegalArgumentError",
				"name",
				"OpenTelemetryFactory.CreateCounter",
				name))
	}
	return NewOpenTelemetryCounter(name, otel.GetMeterProvider().Meter(INSTRUMENTATION_NAME))
}

func (o *OpenTelemetryFactory) CreateGauge(name string) (TelemetryGauge, error) {
	if name == "" {
		return nil, error_util.NewIllegalArgumentError(
			error_util.GetMessage(
				"AwsWrapper.illegalArgumentError",
				"name",
				"OpenTelemetryFactory.CreateGauge",
				name))
	}
	return NewOpenTelemetryGauge(name, otel.GetMeterProvider().Meter(INSTRUMENTATION_NAME))
}
