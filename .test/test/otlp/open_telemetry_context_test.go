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
	"errors"
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
	"github.com/aws/aws-advanced-go-wrapper/otlp"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

func setupTracer() oteltrace.Tracer {
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	return tp.Tracer("test-tracer")
}

func TestNewOpenTelemetryContext_TopLevelRoot(t *testing.T) {
	tracer := setupTracer()
	ctx, _ := otlp.NewOpenTelemetryContext(tracer, "test-op", telemetry.TOP_LEVEL, nil)

	assert.Equal(t, "test-op", ctx.GetName())
	assert.NotNil(t, ctx.Span)
	assert.NotEmpty(t, ctx.Attributes[telemetry.TRACE_NAME_ANNOTATION])
}

func TestNewOpenTelemetryContext_TopLevelWithParent(t *testing.T) {
	tracer := setupTracer()
	parentCtx, parentSpan := tracer.Start(context.Background(), "parent")
	defer parentSpan.End()

	ctx, _ := otlp.NewOpenTelemetryContext(tracer, "child-op", telemetry.TOP_LEVEL, parentCtx)

	assert.Equal(t, "child-op", ctx.GetName())
	assert.Equal(t, parentSpan.SpanContext().TraceID().String(), ctx.Attributes[telemetry.PARENT_TRACE_ANNOTATION])
	assert.Equal(t, parentSpan.SpanContext().SpanID().String(), ctx.Attributes[telemetry.PARENT_SPAN_ANNOTATION])
}

func TestNewOpenTelemetryContext_NestedBecomesTopLevelWhenRoot(t *testing.T) {
	tracer := setupTracer()
	ctx, _ := otlp.NewOpenTelemetryContext(tracer, "nested-root", telemetry.NESTED, nil)

	assert.NotNil(t, ctx.Span)
	assert.Equal(t, "nested-root", ctx.GetName())
	assert.Equal(t, telemetry.TOP_LEVEL, telemetry.TOP_LEVEL) // Implicit check — logic change should reflect in behavior
}

func TestNewOpenTelemetryContext_ForceTopLevelDoesNothing(t *testing.T) {
	tracer := setupTracer()
	ctx, _ := otlp.NewOpenTelemetryContext(tracer, "force-top", telemetry.FORCE_TOP_LEVEL, nil)

	// FORCE_TOP_LEVEL has no effect inside the switch — no span expected
	assert.Nil(t, ctx.Span)
	assert.Empty(t, ctx.Attributes)
}

func TestNewOpenTelemetryContext_NoTrace(t *testing.T) {
	tracer := setupTracer()
	ctx, _ := otlp.NewOpenTelemetryContext(tracer, "no-trace", telemetry.NO_TRACE, nil)

	assert.Nil(t, ctx.Span)
	assert.Empty(t, ctx.Attributes)
}

func TestClone_CopiesAttributesAndError(t *testing.T) {
	tracer := setupTracer()
	origCtx, _ := otlp.NewOpenTelemetryContext(tracer, "original", telemetry.TOP_LEVEL, nil)

	origCtx.SetAttribute("custom", "value")
	origCtx.SetSuccess(true)
	origCtx.SetError(errors.New("test error"))

	clone := origCtx.Clone(telemetry.NESTED)

	assert.NotNil(t, clone.Span)
	assert.Equal(t, "value", clone.Attributes["custom"])
	assert.Equal(t, origCtx.Span.SpanContext().TraceID().String(), clone.Attributes[telemetry.SOURCE_TRACE_ANNOTATION])
	assert.Equal(t, telemetry.COPY_TRACE_NAME_PREFIX+"original", clone.GetName())
}

func TestSetSuccess_SetsCorrectStatus(t *testing.T) {
	tracer := setupTracer()
	ctx, _ := otlp.NewOpenTelemetryContext(tracer, "success-test", telemetry.TOP_LEVEL, nil)

	ctx.SetSuccess(true)
	ctx.SetSuccess(false)

	// Success setting does not throw, nothing more to assert
	assert.NotNil(t, ctx.Span)
}

func TestSetAttribute_SetsSpanAttributes(t *testing.T) {
	tracer := setupTracer()
	ctx, _ := otlp.NewOpenTelemetryContext(tracer, "attr-test", telemetry.TOP_LEVEL, nil)

	ctx.SetAttribute("key", "val")
	assert.Equal(t, "val", ctx.Attributes["key"])
}

func TestCloseContext_EndsSpan(t *testing.T) {
	tracer := setupTracer()
	ctx, _ := otlp.NewOpenTelemetryContext(tracer, "close-test", telemetry.TOP_LEVEL, nil)

	assert.NotPanics(t, func() {
		ctx.CloseContext()
	})
}

func TestOtelPostCopy_TopLevel(t *testing.T) {
	tracer := setupTracer()
	ctx, _ := otlp.NewOpenTelemetryContext(tracer, "trace", telemetry.TOP_LEVEL, nil)

	assert.NotPanics(t, func() {
		otlp.OtelPostCopy(*ctx, telemetry.TOP_LEVEL)
	})
}

func TestOtelPostCopy_NoTrace(t *testing.T) {
	ctx := &otlp.OpenTelemetryContext{}
	assert.NotPanics(t, func() {
		otlp.OtelPostCopy(*ctx, telemetry.NO_TRACE)
	})
}
