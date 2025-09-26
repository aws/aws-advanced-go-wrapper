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

import "context"

type NilTelemetryFactory struct {
	nilTelemetryContext TelemetryContext
	nilTelemetryCounter TelemetryCounter
	nilTelemetryGauge   TelemetryGauge
}

func NewNilTelemetryFactory() *NilTelemetryFactory {
	return &NilTelemetryFactory{
		nilTelemetryContext: NilTelemetryContext{},
		nilTelemetryCounter: NilTelemetryCounter{},
		nilTelemetryGauge:   NilTelemetryGauge{},
	}
}

func (n NilTelemetryFactory) OpenTelemetryContext(_ string, _ TelemetryTraceLevel, ctx context.Context) (TelemetryContext, context.Context) {
	return n.nilTelemetryContext, ctx
}

func (n NilTelemetryFactory) PostCopy(_ TelemetryContext, _ TelemetryTraceLevel) error {
	return nil
}

func (n NilTelemetryFactory) CreateCounter(_ string) (TelemetryCounter, error) {
	return n.nilTelemetryCounter, nil
}

func (n NilTelemetryFactory) CreateGauge(_ string) (TelemetryGauge, error) {
	return n.nilTelemetryGauge, nil
}
