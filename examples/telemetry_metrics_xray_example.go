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

package examples

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-xray-sdk-go/strategy/sampling"
	"github.com/aws/aws-xray-sdk-go/xray"
	"github.com/aws/aws-xray-sdk-go/xraylog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"

	_ "github.com/aws/aws-advanced-go-wrapper/xray"
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	ctx := context.Background()

	endpoint := "localhost:4317"

	metricsExporter, err := otlpmetricgrpc.New(
		ctx,
		otlpmetricgrpc.WithEndpoint(endpoint),
		otlpmetricgrpc.WithInsecure(),
		otlpmetricgrpc.WithTemporalitySelector(metric.DefaultTemporalitySelector),
	)
	if err != nil {
		panic(err)
	}

	reader := metric.NewPeriodicReader(
		metricsExporter,
		metric.WithInterval(1*time.Second),
	)

	resource, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithAttributes(
			attribute.String("service.name", "myservice"),
			attribute.String("service.version", "1.0.0"),
		))
	if err != nil {
		panic(err)
	}

	provider := metric.NewMeterProvider(
		metric.WithReader(reader),
		metric.WithResource(resource),
	)
	otel.SetMeterProvider(provider)

	xray.Configure(xray.Config{
		DaemonAddr: "localhost:2000",
		LogLevel:   "debug",
	})
	xray.SetLogger(xraylog.NewDefaultLogger(os.Stdout, xraylog.LogLevelDebug))
	s, _ := sampling.NewCentralizedStrategyWithFilePath("sampling_rules.json") // Path to local sampling JSON.
	xray.Configure(xray.Config{SamplingStrategy: s})

	dsn := "postgresql://user:password@host:5432/postgres?" +
		"enableTelemetry=true&" +
		"telemetryTracesBackend=XRAY" +
		"&telemetryMetricsBackend=OTLP"

	db, err := sql.Open("awssql-pgx", dsn)
	if err != nil {
		log.Fatal(err)
	}

	db.Ping()

	var id string
	row := db.QueryRow("SELECT pg_catalog.aurora_db_instance_identifier() as id")
	err = row.Scan(&id)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Instance ID:", id)

	closeErr := db.Close()
	if closeErr != nil {
		log.Fatal(closeErr)
	}
}
