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
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
	wrapperOtel "github.com/aws/aws-advanced-go-wrapper/otlp"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"testing"
)

func TestNewOtelContext(t *testing.T) {
	err := SetupTelemetry()
	assert.NoError(t, err)

	tracer := otel.Tracer("testTracer")
	otelCtx0, resultCtx0 := wrapperOtel.NewOpenTelemetryContext(tracer, "test 0", telemetry.NESTED, nil)
	otelCtx1, resultCtx1 := wrapperOtel.NewOpenTelemetryContext(tracer, "test 1", telemetry.NESTED, resultCtx0)
	otelCtx2, resultCtx2 := wrapperOtel.NewOpenTelemetryContext(tracer, "test 2", telemetry.TOP_LEVEL, resultCtx0)

	assert.NotNil(t, resultCtx0)
	assert.NotNil(t, resultCtx1)
	assert.NotNil(t, resultCtx2)
	assert.Equal(t, otelCtx0.Span.SpanContext().TraceID().String(), trace.SpanFromContext(resultCtx0).SpanContext().TraceID().String())
	assert.Equal(t, otelCtx1.Span.SpanContext().TraceID().String(), trace.SpanFromContext(resultCtx1).SpanContext().TraceID().String())
	assert.Equal(t, otelCtx2.Span.SpanContext().TraceID().String(), trace.SpanFromContext(resultCtx2).SpanContext().TraceID().String())
	assert.Equal(t, otelCtx0.Attributes[telemetry.TRACE_NAME_ANNOTATION], "test 0")
	assert.Equal(t, otelCtx1.Attributes[telemetry.TRACE_NAME_ANNOTATION], "test 1")
	assert.Equal(t, otelCtx2.Attributes[telemetry.TRACE_NAME_ANNOTATION], "test 2")
	assert.Equal(t, otelCtx2.Attributes[telemetry.PARENT_TRACE_ANNOTATION], otelCtx0.Span.SpanContext().TraceID().String())
	assert.Equal(t, otelCtx2.Attributes[telemetry.PARENT_SPAN_ANNOTATION], otelCtx0.Span.SpanContext().SpanID().String())
}

func TestOtelClone(t *testing.T) {
	err := SetupTelemetry()
	assert.NoError(t, err)

	tracer := otel.Tracer("testTracer")
	otelCtx, _ := wrapperOtel.NewOpenTelemetryContext(tracer, "test 0", telemetry.NESTED, nil)
	otelCtx.SetAttribute("testAttribute", "testValue")
	spanCopy := otelCtx.Clone(telemetry.TOP_LEVEL)

	assert.Equal(t, telemetry.COPY_TRACE_NAME_PREFIX+otelCtx.GetName(), spanCopy.GetName())
	assert.Equal(t, otelCtx.Attributes["testAttribute"], spanCopy.Attributes["testAttribute"])
	assert.Equal(t, otelCtx.Span.SpanContext().TraceID().String(), spanCopy.Attributes[telemetry.SOURCE_TRACE_ANNOTATION])
}
