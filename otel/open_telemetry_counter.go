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

package otel

import (
	"context"
	"go.opentelemetry.io/otel/metric"
)

type OpenTelemetryCounter struct {
	name    string
	meter   metric.Meter
	counter metric.Int64Counter
}

func NewOpenTelemetryCounter(name string, meter metric.Meter) (*OpenTelemetryCounter, error) {
	newCounter, err := meter.Int64Counter(name)
	if err != nil {
		return nil, err
	}
	return &OpenTelemetryCounter{
		name:    name,
		meter:   meter,
		counter: newCounter,
	}, nil
}

func (o *OpenTelemetryCounter) Add(ctx context.Context, val int64) {
	o.counter.Add(ctx, val)
}

func (o *OpenTelemetryCounter) Inc(ctx context.Context) {
	o.counter.Add(ctx, int64(1))
}
