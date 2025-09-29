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
	"log/slog"
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
	"github.com/aws/aws-advanced-go-wrapper/xray"
	"github.com/stretchr/testify/assert"
)

func TestNewXRayContext(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	err := SetupTelemetry()
	assert.NoError(t, err)

	xrayCtx0, resultCtx0 := xray.NewXRayTelemetryContext("test 0", telemetry.NESTED, nil)
	xrayCtx1, resultCtx1 := xray.NewXRayTelemetryContext("test 1", telemetry.NESTED, resultCtx0)
	xrayCtx2, resultCtx2 := xray.NewXRayTelemetryContext("test 2", telemetry.TOP_LEVEL, resultCtx0)

	assert.NotNil(t, resultCtx0)
	assert.NotNil(t, resultCtx1)
	assert.NotNil(t, resultCtx2)
	assert.Equal(t, "", xrayCtx0.Segment.ParentID)
	assert.Equal(t, xrayCtx0.Segment.ID, xrayCtx1.Segment.ParentID)
	assert.Equal(t, "", xrayCtx2.Segment.ParentID)
	assert.Equal(t, xrayCtx0.Segment.Metadata["default"][telemetry.TRACE_NAME_ANNOTATION], "test 0")
	assert.Equal(t, xrayCtx1.Segment.Metadata["default"][telemetry.TRACE_NAME_ANNOTATION], "test 1")
	assert.Equal(t, xrayCtx2.Segment.Metadata["default"][telemetry.TRACE_NAME_ANNOTATION], "test 2")
}

func TestXRayClone(t *testing.T) {
	err := SetupTelemetry()
	assert.NoError(t, err)

	xrayCtx, _ := xray.NewXRayTelemetryContext("test 0", telemetry.NESTED, nil)
	xrayCtx.SetAttribute("testAttribute", "testValue")
	spanCopy := xrayCtx.Clone(telemetry.TOP_LEVEL)

	assert.Equal(t, telemetry.COPY_TRACE_NAME_PREFIX+xrayCtx.GetName(), spanCopy.GetName())
	assert.Equal(t, xrayCtx.Segment.Metadata["testAttribute"], spanCopy.Segment.Metadata["testAttribute"])
	assert.Equal(t, xrayCtx.Segment.TraceID, spanCopy.Segment.Metadata["default"][telemetry.SOURCE_TRACE_ANNOTATION])
}
