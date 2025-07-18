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

package otlp_test

import (
	"context"
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
	"github.com/aws/aws-advanced-go-wrapper/otlp"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func init() {
	// Set no-op test providers
	otel.SetTracerProvider(sdktrace.NewTracerProvider())
	otel.SetMeterProvider(sdkmetric.NewMeterProvider())
}

func TestOpenTelemetryFactory_OpenTelemetryContext(t *testing.T) {
	factory := otlp.OpenTelemetryFactory{}
	name := "test-span"
	ctx := context.Background()

	otelCtx, newCtx := factory.OpenTelemetryContext(name, telemetry.TOP_LEVEL, ctx)

	assert.NotNil(t, otelCtx)
	assert.NotNil(t, newCtx)
	assert.Equal(t, name, otelCtx.GetName())
}

func TestOpenTelemetryFactory_PostCopy_Success(t *testing.T) {
	factory := otlp.OpenTelemetryFactory{}
	ctx := context.Background()

	otelCtx, _ := factory.OpenTelemetryContext("copy-span", telemetry.TOP_LEVEL, ctx)
	err := factory.PostCopy(otelCtx, telemetry.TOP_LEVEL)

	assert.NoError(t, err)
}

func TestOpenTelemetryFactory_CreateCounter_Success(t *testing.T) {
	factory := otlp.OpenTelemetryFactory{}

	counter, err := factory.CreateCounter("test_counter")

	assert.NoError(t, err)
	assert.NotNil(t, counter)
}

func TestOpenTelemetryFactory_CreateCounter_InvalidName(t *testing.T) {
	factory := otlp.OpenTelemetryFactory{}

	counter, err := factory.CreateCounter("")

	assert.Nil(t, counter)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CreateCounter")
}

func TestOpenTelemetryFactory_CreateGauge_Success(t *testing.T) {
	factory := otlp.OpenTelemetryFactory{}

	gauge, err := factory.CreateGauge("test_gauge")

	assert.NoError(t, err)
	assert.NotNil(t, gauge)
}

func TestOpenTelemetryFactory_CreateGauge_InvalidName(t *testing.T) {
	factory := otlp.OpenTelemetryFactory{}

	gauge, err := factory.CreateGauge("")

	assert.Nil(t, gauge)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CreateGauge")
}
