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
	"fmt"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"log/slog"
)

type OpenTelemetryContext struct {
	name       string
	tracer     trace.Tracer
	Span       trace.Span
	Attributes map[string]string
	statusCode codes.Code
	err        error
}

func NewOpenTelemetryContext(tracer trace.Tracer, name string, traceLevel telemetry.TelemetryTraceLevel, ctx context.Context) (*OpenTelemetryContext, context.Context) {
	var span trace.Span
	var parentSpan trace.Span
	var resultCtx context.Context

	isRoot := true
	if ctx != nil {
		isRoot = false
		parentSpan = trace.SpanFromContext(ctx)
	} else {
		ctx = context.TODO()
	}

	effectiveTraceLevel := traceLevel
	if isRoot && effectiveTraceLevel == telemetry.NESTED {
		effectiveTraceLevel = telemetry.TOP_LEVEL
	}

	otelCtx := &OpenTelemetryContext{
		name:       name,
		tracer:     tracer,
		Attributes: map[string]string{},
	}

	switch effectiveTraceLevel {
	case telemetry.FORCE_TOP_LEVEL:
	case telemetry.TOP_LEVEL:
		resultCtx, span = tracer.Start(ctx, name)
		otelCtx.Span = span
		if !isRoot {
			otelCtx.SetAttribute(telemetry.PARENT_TRACE_ANNOTATION, parentSpan.SpanContext().TraceID().String())
			otelCtx.SetAttribute(telemetry.PARENT_SPAN_ANNOTATION, parentSpan.SpanContext().SpanID().String())
		}
		otelCtx.SetAttribute(telemetry.TRACE_NAME_ANNOTATION, name)
		slog.Debug(fmt.Sprintf("[OTLP] Telemetry '%s' trace ID: '%s'.", name, span.SpanContext().TraceID().String()))
	case telemetry.NESTED:
		resultCtx, span = tracer.Start(ctx, name)
		otelCtx.Span = span
		otelCtx.SetAttribute(telemetry.TRACE_NAME_ANNOTATION, name)
	case telemetry.NO_TRACE:
		// Do not post this context.
		break
	default:
		break
	}

	return otelCtx, resultCtx
}

func OtelPostCopy(telemetryCtx OpenTelemetryContext, traceLevel telemetry.TelemetryTraceLevel) {
	if traceLevel == telemetry.NO_TRACE {
		return
	}

	if traceLevel == telemetry.FORCE_TOP_LEVEL || traceLevel == telemetry.TOP_LEVEL {
		ctx := telemetryCtx.Clone(traceLevel)
		ctx.CloseContext()
	}
}

func (o *OpenTelemetryContext) Clone(traceLevel telemetry.TelemetryTraceLevel) *OpenTelemetryContext {
	copyCtx, _ := NewOpenTelemetryContext(o.tracer, telemetry.COPY_TRACE_NAME_PREFIX+o.name, traceLevel, nil)

	for key, value := range o.Attributes {
		if key != telemetry.TRACE_NAME_ANNOTATION {
			copyCtx.SetAttribute(key, value)
		}
	}

	if o.statusCode == codes.Ok {
		copyCtx.SetSuccess(true)
	} else if o.statusCode == codes.Error {
		copyCtx.SetSuccess(false)
	}

	if o.err != nil {
		copyCtx.SetError(o.err)
	}

	copyCtx.SetAttribute(telemetry.SOURCE_TRACE_ANNOTATION, o.Span.SpanContext().TraceID().String())
	return copyCtx
}

func (o *OpenTelemetryContext) SetSuccess(success bool) {
	if o.Span != nil {
		if success {
			o.Span.SetStatus(codes.Ok, "")
		} else {
			o.Span.SetStatus(codes.Error, "")
		}
	}
}

func (o *OpenTelemetryContext) SetAttribute(key string, value string) {
	if o.Span != nil {
		o.Span.SetAttributes(attribute.String(key, value))
		o.Attributes[key] = value
	}
}

func (o *OpenTelemetryContext) SetError(err error) {
	if o.Span != nil && err != nil {
		o.Span.SetAttributes(attribute.String(telemetry.ERROR_TYPE_ANNOTATION, utils.GetStructName(err)))
		o.Span.SetAttributes(attribute.String(telemetry.ERROR_MESSAGE_ANNOTATION, err.Error()))
		o.Span.RecordError(err)
	}
}

func (o *OpenTelemetryContext) GetName() string {
	return o.name
}

func (o *OpenTelemetryContext) CloseContext() {
	if o.Span != nil {
		o.Span.End()
	}
}
