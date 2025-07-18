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
	"context"
	"strings"
	"testing"

	mock_telemetry "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/util/telemetry"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func setUpProps(enableTelemetry bool, tracesBackend, metricsBackend string, submitTopLevel bool) map[string]string {
	return map[string]string{
		property_util.ENABLE_TELEMETRY.Name:           strings.ToLower(strings.TrimSpace(strings.Title(enableTelemetryString(enableTelemetry)))),
		property_util.TELEMETRY_TRACES_BACKEND.Name:   tracesBackend,
		property_util.TELEMETRY_METRICS_BACKEND.Name:  metricsBackend,
		property_util.TELEMETRY_SUBMIT_TOP_LEVEL.Name: strings.ToLower(strings.TrimSpace(strings.Title(enableTelemetryString(submitTopLevel)))),
	}
}

func enableTelemetryString(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func TestNewDefaultTelemetryFactory_ValidOTLP(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTraceFactory := mock_telemetry.NewMockTelemetryFactory(ctrl)
	mockMetricsFactory := mock_telemetry.NewMockTelemetryFactory(ctrl)

	telemetry.UseTelemetryFactory("otlp", mockTraceFactory)
	telemetry.UseTelemetryFactory("xray", mockMetricsFactory)

	props := setUpProps(true, "otlp", "xray", true)
	factory, err := telemetry.NewDefaultTelemetryFactory(props)

	assert.NoError(t, err)
	assert.NotNil(t, factory)
}

func TestNewDefaultTelemetryFactory_InvalidBackend(t *testing.T) {
	props := setUpProps(true, "invalid-backend", "none", true)
	factory, err := telemetry.NewDefaultTelemetryFactory(props)

	assert.Nil(t, factory)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid-backend")
}

func TestNewDefaultTelemetryFactory_DisabledTelemetry(t *testing.T) {
	props := setUpProps(false, "anything", "anything", true)
	factory, err := telemetry.NewDefaultTelemetryFactory(props)

	assert.NoError(t, err)
	assert.NotNil(t, factory)
}

func TestOpenTelemetryContext_TopLevelTraceDisabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTraceFactory := mock_telemetry.NewMockTelemetryFactory(ctrl)
	mockContext := mock_telemetry.NewMockTelemetryContext(ctrl)

	telemetry.UseTelemetryFactory("otlp", mockTraceFactory)

	props := setUpProps(true, "otlp", "none", false)
	factory, _ := telemetry.NewDefaultTelemetryFactory(props)

	mockTraceFactory.
		EXPECT().
		OpenTelemetryContext("test", telemetry.NESTED, gomock.Any()).
		Return(mockContext, context.TODO())

	ctx, _ := factory.OpenTelemetryContext("test", telemetry.TOP_LEVEL, context.TODO())
	assert.NotNil(t, ctx)
}

func TestPostCopy_WithFactory(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTraceFactory := mock_telemetry.NewMockTelemetryFactory(ctrl)
	mockContext := mock_telemetry.NewMockTelemetryContext(ctrl)

	telemetry.UseTelemetryFactory("otlp", mockTraceFactory)

	props := setUpProps(true, "otlp", "none", true)
	factory, _ := telemetry.NewDefaultTelemetryFactory(props)

	mockTraceFactory.EXPECT().
		PostCopy(mockContext, telemetry.TOP_LEVEL).
		Return(nil)

	err := factory.PostCopy(mockContext, telemetry.TOP_LEVEL)
	assert.NoError(t, err)
}

func TestCreateCounter_WithFactory(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMetricsFactory := mock_telemetry.NewMockTelemetryFactory(ctrl)
	mockCounter := mock_telemetry.NewMockTelemetryCounter(ctrl)

	telemetry.UseTelemetryFactory("otlp", mockMetricsFactory)

	props := setUpProps(true, "none", "otlp", true)
	factory, _ := telemetry.NewDefaultTelemetryFactory(props)

	mockMetricsFactory.EXPECT().
		CreateCounter("counter").
		Return(mockCounter, nil)

	counter, err := factory.CreateCounter("counter")
	assert.NoError(t, err)
	assert.NotNil(t, counter)
}

func TestCreateGauge_WithFactory(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMetricsFactory := mock_telemetry.NewMockTelemetryFactory(ctrl)
	mockGauge := mock_telemetry.NewMockTelemetryGauge(ctrl)

	telemetry.UseTelemetryFactory("otlp", mockMetricsFactory)

	props := setUpProps(true, "none", "otlp", true)
	factory, _ := telemetry.NewDefaultTelemetryFactory(props)

	mockMetricsFactory.EXPECT().
		CreateGauge("gauge").
		Return(mockGauge, nil)

	gauge, err := factory.CreateGauge("gauge")
	assert.NoError(t, err)
	assert.NotNil(t, gauge)
}
