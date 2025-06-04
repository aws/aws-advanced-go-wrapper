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

import "context"

type TelemetryTraceLevel string

const (
	FORCE_TOP_LEVEL TelemetryTraceLevel = "FORCE_TOP_LEVEL"
	TOP_LEVEL       TelemetryTraceLevel = "TOP_LEVEL"
	NESTED          TelemetryTraceLevel = "NESTED"
	NO_TRACE        TelemetryTraceLevel = "NO_TRACE"
)

const (
	INSTRUMENTATION_NAME     = "aws-advanced-go-wrapper"
	TRACE_NAME_ANNOTATION    = "traceName"
	SOURCE_TRACE_ANNOTATION  = "sourceTraceId"
	PARENT_TRACE_ANNOTATION  = "parentTraceId"
	PARENT_SPAN_ANNOTATION   = "parentSpanId"
	ERROR_TYPE_ANNOTATION    = "errorType"
	ERROR_MESSAGE_ANNOTATION = "errorMessage"
	COPY_TRACE_NAME_PREFIX   = "copy: "

	TELEMETRY_OPEN_CONNECTION    = "Aws Advanced Go Wrapper - Open"
	TELEMETRY_EXECUTE            = "AWS Advanced Go Wrapper - %s"
	TELEMETRY_INIT_HOST_PROVIDER = "initHostProvider"
	TELEMETRY_CONNECT            = "connect"
	TELEMETRY_CONNECT_INTERNAL   = "%s - connect"
	TELEMETRY_FETCH_TOKEN        = "fetch authentication token"
	TELEMETRY_UPDATE_SECRETS     = "fetch credentials"
	TELEMETRY_CONN_STATUS_CHECK  = "connection status check"
	TELEMETRY_WRITER_FAILOVER    = "failover to writer host"
	TELEMETRY_READER_FAILOVER    = "failover to reader host"
	TELEMETRY_FETCH_SAML_ADFS    = "fetch ADFS SAML Assertion"
	TELEMETRY_FETCH_SAML_OKTA    = "fetch OKTA SAML Assertion"

	TELEMETRY_ATTRIBUTE_URL      = "url"
	TELEMETRY_ATTRIBUTE_SQL_CALL = "sqlCall"
)

type TelemetryCounter interface {
	Add(ctx context.Context, val int64)
	Inc(ctx context.Context)
}

type TelemetryGauge interface{}

type TelemetryContext interface {
	SetSuccess(success bool)
	SetAttribute(key string, value string)
	SetError(err error)
	GetName() string
	CloseContext()
}

type TelemetryFactory interface {
	OpenTelemetryContext(name string, traceLevel TelemetryTraceLevel, ctx context.Context) (TelemetryContext, context.Context)
	PostCopy(telemetryContext TelemetryContext, traceLevel TelemetryTraceLevel) error
	CreateCounter(name string) (TelemetryCounter, error)
	CreateGauge(name string) (TelemetryGauge, error)
}
