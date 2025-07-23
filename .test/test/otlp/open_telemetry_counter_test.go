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

	"github.com/aws/aws-advanced-go-wrapper/otlp"
	"github.com/stretchr/testify/assert"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

func TestNewOpenTelemetryCounter_Success(t *testing.T) {
	provider := sdkmetric.NewMeterProvider()
	meter := provider.Meter("test-meter")

	counter, err := otlp.NewOpenTelemetryCounter("my_counter", meter)

	assert.NoError(t, err)
	assert.NotNil(t, counter)
}

func TestOpenTelemetryCounter_Add(t *testing.T) {
	provider := sdkmetric.NewMeterProvider()
	meter := provider.Meter("test-meter")

	counter, err := otlp.NewOpenTelemetryCounter("add_counter", meter)
	assert.NoError(t, err)

	counter.Add(context.Background(), 10)
}

func TestOpenTelemetryCounter_Inc(t *testing.T) {
	provider := sdkmetric.NewMeterProvider()
	meter := provider.Meter("test-meter")

	counter, err := otlp.NewOpenTelemetryCounter("inc_counter", meter)
	assert.NoError(t, err)

	counter.Inc(context.Background())
}
