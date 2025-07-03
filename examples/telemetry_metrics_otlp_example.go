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

package main

import (
	"context"
	"database/sql"
	"fmt"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	"log"
	"log/slog"
	"time"

	_ "github.com/aws/aws-advanced-go-wrapper/otlp"
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	ctx := context.Background()

	endpoint := "localhost:4317"

	exporter, err := otlptracegrpc.New(
		ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		panic(err)
	}

	bsp := trace.NewBatchSpanProcessor(exporter,
		trace.WithMaxQueueSize(10_000),
		trace.WithMaxExportBatchSize(10_000))
	// Call shutdown to flush the buffers when program exits.
	defer bsp.Shutdown(ctx)

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

	tracerProvider := trace.NewTracerProvider(
		trace.WithSampler(trace.AlwaysSample()),
		trace.WithResource(resource),
	)
	tracerProvider.RegisterSpanProcessor(bsp)

	// Install our tracer provider and we are done.
	otel.SetTracerProvider(tracerProvider)

	provider := metric.NewMeterProvider(
		metric.WithReader(reader),
		metric.WithResource(resource),
	)
	otel.SetMeterProvider(provider)

	dsn := "postgresql://user:password@host:5432/postgres?" +
		"enableTelemetry=true&" +
		"telemetryTracesBackend=OTLP&" +
		"telemetryMetricsBackend=OTLP"

	db, err := sql.Open("awssql-pgx", dsn)
	if err != nil {
		log.Fatal(err)
	}

	db.Ping()

	var id string
	row := db.QueryRow("SELECT aurora_db_instance_identifier() as id")
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
