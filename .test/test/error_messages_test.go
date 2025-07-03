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
	"log/slog"
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"

	"github.com/stretchr/testify/assert"
)

func TestValidateNumericalProps(t *testing.T) {
	props := map[string]string{
		property_util.PORT.Name:                             "1234",
		property_util.CLUSTER_TOPOLOGY_REFRESH_RATE_MS.Name: "-30",
		property_util.IAM_EXPIRATION_SEC.Name:               "-1",
		property_util.FAILURE_DETECTION_INTERVAL_MS.Name:    "-10",
		property_util.HTTP_TIMEOUT_MS.Name:                  "0",
	}

	// When a positive int value is required, throws error if value is negative.
	val, err := property_util.GetPositiveIntProperty(props, property_util.FAILURE_DETECTION_INTERVAL_MS)
	assert.Equal(t, 0, val)
	assert.NotNil(t, err)
	assert.Equal(t, error_util.GetMessage("AwsWrapperProperty.requiresNonNegativeIntValue", property_util.FAILURE_DETECTION_INTERVAL_MS.Name), err.Error())

	val, err = property_util.GetPositiveIntProperty(props, property_util.PORT)
	assert.Equal(t, 1234, val)
	assert.Nil(t, err)

	// When a positive, non-zero int value is recommended, log a warning if value is <= 0.
	handler := &TestHandler{}
	slog.SetDefault(slog.New(handler))

	val = property_util.GetRefreshRateValue(props, property_util.PORT)
	assert.Equal(t, 1234, val)
	assert.Equal(t, 0, len(handler.records))

	val = property_util.GetHttpTimeoutValue(map[string]string{property_util.HTTP_TIMEOUT_MS.Name: "10"})
	assert.Equal(t, 10, val)
	assert.Equal(t, 0, len(handler.records))

	val = property_util.GetExpirationValue(props, property_util.PORT)
	assert.Equal(t, 1234, val)
	assert.Equal(t, 0, len(handler.records))

	val = property_util.GetRefreshRateValue(props, property_util.CLUSTER_TOPOLOGY_REFRESH_RATE_MS)
	assert.Equal(t, -30, val)
	assert.Equal(t, 1, len(handler.records))
	assert.Equal(t, error_util.GetMessage("AwsWrapperProperty.noRefreshRateValue", property_util.CLUSTER_TOPOLOGY_REFRESH_RATE_MS.Name, val), handler.records[0].Message)

	val = property_util.GetHttpTimeoutValue(props)
	assert.Equal(t, 0, val)
	assert.Equal(t, 2, len(handler.records))
	assert.Equal(t, error_util.GetMessage("AwsWrapperProperty.noTimeoutValue", property_util.HTTP_TIMEOUT_MS.Name, val), handler.records[1].Message)

	val = property_util.GetExpirationValue(props, property_util.IAM_EXPIRATION_SEC)
	assert.Equal(t, -1, val)
	assert.Equal(t, 3, len(handler.records))
	assert.Equal(t, error_util.GetMessage("AwsWrapperProperty.noExpirationValue", property_util.IAM_EXPIRATION_SEC.Name, val), handler.records[2].Message)
}

type TestHandler struct {
	records []slog.Record
}

func (h *TestHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *TestHandler) Handle(ctx context.Context, r slog.Record) error {
	h.records = append(h.records, r)
	return nil
}

func (h *TestHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}

func (h *TestHandler) WithGroup(name string) slog.Handler {
	return h
}
