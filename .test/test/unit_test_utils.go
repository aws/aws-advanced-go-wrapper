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
	"errors"
	"os"
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
	mysql_driver "github.com/aws/aws-advanced-go-wrapper/mysql-driver"
	"github.com/aws/aws-xray-sdk-go/strategy/sampling"
	"github.com/aws/aws-xray-sdk-go/xray"
	"github.com/aws/aws-xray-sdk-go/xraylog"
	xray2 "go.opentelemetry.io/contrib/propagators/aws/xray"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdkTrace "go.opentelemetry.io/otel/sdk/trace"
)

var emptyProps = utils.NewRWMap[string, string]()

func MakeMapFromKeysAndVals(keysAndVals ...string) *utils.RWMap[string, string] {
	result := utils.NewRWMap[string, string]()

	// If there's an odd number of elements, we'll skip the last one.
	for i := 0; i < len(keysAndVals)-1; i += 2 {
		key := keysAndVals[i]
		val := keysAndVals[i+1]
		result.Put(key, val)
	}

	return result
}

func GetValueOrEmptyString(rwMap *utils.RWMap[string, string], key string) string {
	val, _ := rwMap.Get(key)
	return val
}

func readFile(t *testing.T, fileName string) []byte {
	content, err := os.ReadFile(fileName)
	if err != nil {
		t.Fatalf("Error reading from file: '%s'.", err.Error())
	}
	return content
}

func SetupTelemetry() error {
	ctx := context.Background()

	endpoint := "localhost:4317"

	exporter, err := otlptracegrpc.New(
		ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return errors.New("unable to create otlp trace exporter")
	}

	bsp := sdkTrace.NewBatchSpanProcessor(exporter,
		sdkTrace.WithMaxQueueSize(10_000),
		sdkTrace.WithMaxExportBatchSize(10_000))
	defer func() { _ = bsp.Shutdown(ctx) }()

	newResource, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithAttributes(
			attribute.String("service.name", "aws-advanced-go-wrapper-unit-tests"),
			attribute.String("service.version", "1.0.0"),
		))
	if err != nil {
		return err
	}

	tracerProvider := sdkTrace.NewTracerProvider(
		sdkTrace.WithResource(newResource),
		sdkTrace.WithIDGenerator(xray2.NewIDGenerator()),
	)
	tracerProvider.RegisterSpanProcessor(bsp)

	otel.SetTracerProvider(tracerProvider)

	xray.SetLogger(xraylog.NewDefaultLogger(os.Stdout, xraylog.LogLevelDebug))
	s, _ := sampling.NewCentralizedStrategyWithFilePath("./resources/sampling_rules.json") // path to local sampling json
	err = xray.Configure(xray.Config{SamplingStrategy: s})
	if err != nil {
		return err
	}

	return nil
}

func CreateMockPluginService(props *utils.RWMap[string, string]) driver_infrastructure.PluginService {
	mockTargetDriver := &MockTargetDriver{}
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	mockPluginManager := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, props, driver_infrastructure.ConnectionProviderManager{}, telemetryFactory)
	pluginServiceImpl, _ := plugin_helpers.NewPluginServiceImpl(mockPluginManager, mysql_driver.NewMySQLDriverDialect(), props, mysqlTestDsn)
	return pluginServiceImpl
}
