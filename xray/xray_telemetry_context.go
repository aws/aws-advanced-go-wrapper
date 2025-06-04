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

package xray

import (
	"context"
	"fmt"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
	"github.com/aws/aws-xray-sdk-go/xray"
	"log/slog"
)

type XRayTelemetryContext struct {
	name    string
	Segment *xray.Segment
	err     error
}

func NewXRayTelemetryContext(name string, traceLevel telemetry.TelemetryTraceLevel, ctx context.Context) (*XRayTelemetryContext, context.Context) {
	var Segment *xray.Segment
	var parentSegment *xray.Segment
	var resultCtx context.Context

	if ctx != nil {
		parentSegment = xray.GetSegment(ctx)
	} else {
		ctx = context.TODO()
	}

	effectiveTraceLevel := traceLevel
	if parentSegment == nil && effectiveTraceLevel == telemetry.NESTED {
		effectiveTraceLevel = telemetry.TOP_LEVEL
	}

	xrayCtx := &XRayTelemetryContext{
		name: name,
	}

	switch effectiveTraceLevel {
	case telemetry.FORCE_TOP_LEVEL:
	case telemetry.TOP_LEVEL:
		resultCtx, Segment = xray.BeginSegment(ctx, name)
		xrayCtx.Segment = Segment
		if parentSegment != nil {
			xrayCtx.SetAttribute(telemetry.PARENT_TRACE_ANNOTATION, parentSegment.ParentID)
			xrayCtx.SetAttribute(telemetry.PARENT_SPAN_ANNOTATION, parentSegment.ID)
		}
		xrayCtx.SetAttribute(telemetry.TRACE_NAME_ANNOTATION, name)
		slog.Debug(fmt.Sprintf("[XRay] Telemetry '%s' trace ID: '%s'. SEGMENT: %s", name, Segment.TraceID, Segment.ID))
	case telemetry.NESTED:
		resultCtx, Segment = xray.BeginSubsegment(ctx, name)
		xrayCtx.Segment = Segment
		xrayCtx.SetAttribute(telemetry.TRACE_NAME_ANNOTATION, name)
	case telemetry.NO_TRACE:
		// Do not post this trace.
		break
	default:
		break
	}

	return xrayCtx, resultCtx
}

func XRayPostCopy(telemetryCtx XRayTelemetryContext, traceLevel telemetry.TelemetryTraceLevel) {
	if traceLevel == telemetry.NO_TRACE {
		return
	}

	if traceLevel == telemetry.FORCE_TOP_LEVEL || traceLevel == telemetry.TOP_LEVEL {
		ctx := telemetryCtx.Clone(traceLevel)
		ctx.CloseContext()
	}
}

func (x *XRayTelemetryContext) Clone(traceLevel telemetry.TelemetryTraceLevel) *XRayTelemetryContext {
	copyCtx, _ := NewXRayTelemetryContext(telemetry.COPY_TRACE_NAME_PREFIX+x.name, traceLevel, nil)

	for _, attributes := range x.Segment.Metadata {
		for key, value := range attributes {
			if key != telemetry.TRACE_NAME_ANNOTATION {
				attribute, ok := value.(string)
				if ok {
					copyCtx.SetAttribute(key, attribute)
				}
			}
		}
	}

	if x.err != nil {
		copyCtx.SetError(x.err)
	}

	copyCtx.SetAttribute(telemetry.SOURCE_TRACE_ANNOTATION, x.Segment.TraceID)
	return copyCtx
}

func (x *XRayTelemetryContext) SetSuccess(success bool) {
	// Segments do not have a set success method.
}

func (x *XRayTelemetryContext) SetAttribute(key string, value string) {
	if x.Segment != nil {
		_ = x.Segment.AddMetadata(key, value)
	}
}

func (x *XRayTelemetryContext) SetError(err error) {
	if x.Segment != nil && err != nil {
		x.SetAttribute("errorType", fmt.Sprintf("%T", err))
		x.SetAttribute("errorMessage", err.Error())
	}
}

func (x *XRayTelemetryContext) GetName() string {
	return x.name
}

func (x *XRayTelemetryContext) CloseContext() {
	if x.Segment != nil {
		x.Segment.Close(nil)
	}
}
