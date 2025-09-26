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
	"context"

	"strings"

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

var availableTelemetryFactories = map[string]TelemetryFactory{}

func UseTelemetryFactory(name string, factory TelemetryFactory) {
	availableTelemetryFactories[name] = factory
}

type DefaultTelemetryFactory struct {
	telemetrySubmitTopLevel bool
	tracesTelemetryFactory  TelemetryFactory
	metricsTelemetryFactory TelemetryFactory
}

func NewDefaultTelemetryFactory(props *utils.RWMap[string]) (*DefaultTelemetryFactory, error) {
	enableTelemetry := property_util.GetVerifiedWrapperPropertyValue[bool](props, property_util.ENABLE_TELEMETRY)
	tracesTelemetryBackend := strings.ToLower(property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.TELEMETRY_TRACES_BACKEND))
	metricsTelemetryBackend := strings.ToLower(property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.TELEMETRY_METRICS_BACKEND))
	telemetrySubmitTopLevel := property_util.GetVerifiedWrapperPropertyValue[bool](props, property_util.TELEMETRY_SUBMIT_TOP_LEVEL)

	tracesTelemetryFactory, err := getTelemetryFactory(enableTelemetry, tracesTelemetryBackend)
	if err != nil {
		return nil, err
	}
	metricsTelemetryFactory, err := getTelemetryFactory(enableTelemetry, metricsTelemetryBackend)
	if err != nil {
		return nil, err
	}

	return &DefaultTelemetryFactory{
		tracesTelemetryFactory:  tracesTelemetryFactory,
		metricsTelemetryFactory: metricsTelemetryFactory,
		telemetrySubmitTopLevel: telemetrySubmitTopLevel,
	}, nil
}

func getTelemetryFactory(enableTelemetry bool, backend string) (TelemetryFactory, error) {
	if enableTelemetry {
		if backend == "otlp" || backend == "xray" {
			factory, ok := availableTelemetryFactories[backend]
			if ok {
				return factory, nil
			}
			return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("DefaultTelemetryFactory.telemetryFactoryUnavailable", backend))
		} else if backend == "none" {
			return NewNilTelemetryFactory(), nil
		} else {
			return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("DefaultTelemetryFactory.invalidBackend", backend))
		}
	} else {
		return NewNilTelemetryFactory(), nil
	}
}

func (d *DefaultTelemetryFactory) OpenTelemetryContext(name string, traceLevel TelemetryTraceLevel, ctx context.Context) (TelemetryContext, context.Context) {
	effectiveTraceLevel := traceLevel
	if !d.telemetrySubmitTopLevel && traceLevel == TOP_LEVEL {
		effectiveTraceLevel = NESTED
	}
	return d.tracesTelemetryFactory.OpenTelemetryContext(name, effectiveTraceLevel, ctx)
}

func (d *DefaultTelemetryFactory) PostCopy(telemetryContext TelemetryContext, traceLevel TelemetryTraceLevel) error {
	if d.tracesTelemetryFactory != nil {
		return d.tracesTelemetryFactory.PostCopy(telemetryContext, traceLevel)
	}
	return error_util.NewGenericAwsWrapperError(error_util.GetMessage("DefaultTelemetryFactory.missingTelemetryFactory", "tracesTelemetryFactory"))
}

func (d *DefaultTelemetryFactory) CreateCounter(name string) (TelemetryCounter, error) {
	if d.metricsTelemetryFactory != nil {
		return d.metricsTelemetryFactory.CreateCounter(name)
	}
	return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("DefaultTelemetryFactory.missingTelemetryFactory", "metricsTelemetryFactory"))
}

func (d *DefaultTelemetryFactory) CreateGauge(name string) (TelemetryGauge, error) {
	if d.metricsTelemetryFactory != nil {
		return d.metricsTelemetryFactory.CreateGauge(name)
	}
	return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("DefaultTelemetryFactory.missingTelemetryFactory", "metricsTelemetryFactory"))
}
