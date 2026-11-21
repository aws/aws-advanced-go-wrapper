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

package error_util

import (
	"database/sql/driver"
	"errors"
)

type AwsWrapperErrorType int

const (
	GenericAwsWrapperErrorType            AwsWrapperErrorType = 0
	UnsupportedStrategyErrorType          AwsWrapperErrorType = 1
	UnsupportedMethodErrorType            AwsWrapperErrorType = 2
	IllegalArgumentErrorType              AwsWrapperErrorType = 3
	LoginErrorType                        AwsWrapperErrorType = 4
	InternalQueryTimeoutErrorType         AwsWrapperErrorType = 5
	UnavailableHostErrorType              AwsWrapperErrorType = 6
	DsnParsingErrorType                   AwsWrapperErrorType = 7
	TimeoutErrorType                      AwsWrapperErrorType = 8
	ConnectionLifetimeClosedErrorType     AwsWrapperErrorType = 9
	ConnectionLifetimeFailedErrorType     AwsWrapperErrorType = 10
	FailoverSuccessErrorType              AwsWrapperErrorType = 300
	FailoverFailedErrorType               AwsWrapperErrorType = 301
	TransactionResolutionUnknownErrorType AwsWrapperErrorType = 302
)

type AwsWrapperError struct {
	Message   string
	ErrorType AwsWrapperErrorType
	// Cause is the error that led to this one, if any. It is reachable through errors.Is and errors.As.
	Cause error
}

func (a *AwsWrapperError) Error() string {
	return a.Message
}

func (a *AwsWrapperError) Unwrap() error {
	return a.Cause
}

func (a *AwsWrapperError) IsType(errorType AwsWrapperErrorType) bool {
	return a.ErrorType == errorType
}

func IsType(err error, errorType AwsWrapperErrorType) bool {
	var awsWrapperError *AwsWrapperError
	ok := errors.As(err, &awsWrapperError)
	if ok {
		return awsWrapperError.IsType(errorType)
	}
	return false
}

func (a *AwsWrapperError) IsFailoverErrorType() bool {
	return a.ErrorType >= 300
}

func NewGenericAwsWrapperError(message string) *AwsWrapperError {
	return &AwsWrapperError{Message: message, ErrorType: GenericAwsWrapperErrorType}
}

func NewTimeoutError(message string) *AwsWrapperError {
	return &AwsWrapperError{Message: message, ErrorType: TimeoutErrorType}
}

func NewUnsupportedStrategyError(message string) *AwsWrapperError {
	return &AwsWrapperError{
		Message:   message,
		ErrorType: UnsupportedStrategyErrorType}
}

func NewUnsupportedMethodError(method string, object string) *AwsWrapperError {
	return &AwsWrapperError{Message: GetMessage("AwsWrapper.unsupportedMethodError", method, object), ErrorType: UnsupportedStrategyErrorType}
}

func NewIllegalArgumentError(message string) *AwsWrapperError {
	return &AwsWrapperError{Message: message, ErrorType: IllegalArgumentErrorType}
}

var FailoverSuccessError = &AwsWrapperError{Message: GetMessage("Failover.connectionChangedError"), ErrorType: FailoverSuccessErrorType}

func NewFailoverFailedError(message string) *AwsWrapperError {
	return &AwsWrapperError{Message: message, ErrorType: FailoverFailedErrorType}
}

var TransactionResolutionUnknownError = &AwsWrapperError{
	Message:   GetMessage("Failover.transactionResolutionUnknownError"),
	ErrorType: TransactionResolutionUnknownErrorType}

func NewLoginError(message string) *AwsWrapperError {
	return &AwsWrapperError{Message: message, ErrorType: LoginErrorType}
}

var InternalQueryTimeoutError = &AwsWrapperError{Message: GetMessage("Failover.timeoutError"), ErrorType: InternalQueryTimeoutErrorType}

func NewUnavailableHostError(host string) *AwsWrapperError {
	return &AwsWrapperError{Message: GetMessage("HostMonitoringConnectionPlugin.unavailableHost", host), ErrorType: UnavailableHostErrorType}
}

func NewDsnParsingError(message string) *AwsWrapperError {
	return &AwsWrapperError{Message: message, ErrorType: DsnParsingErrorType}
}

var ShouldNotBeCalledError = &AwsWrapperError{Message: "Shouldn't be called.", ErrorType: GenericAwsWrapperErrorType}

// ConnectionLifetimeSuccessError reports that the connection lifetime plugin replaced an expired physical connection.
// The pending call was not executed; the connection is usable and the call can be retried after restoring session state.
// It has the same error type as FailoverSuccessError, so code that already retries after a successful failover handles
// it without change.
var ConnectionLifetimeSuccessError = &AwsWrapperError{
	Message:   GetMessage("ConnectionLifetimePlugin.connectionChanged"),
	ErrorType: FailoverSuccessErrorType}

// NewConnectionLifetimeClosedError reports a connection closed because it reached its maximum lifetime.
// It wraps driver.ErrBadConn because the pending call was not sent, so database/sql discards the connection and
// retries on another one. closeErr, when not nil, is the error returned while closing the physical connection.
func NewConnectionLifetimeClosedError(closeErr error) *AwsWrapperError {
	return &AwsWrapperError{
		Message:   GetMessage("ConnectionLifetimePlugin.connectionClosed"),
		ErrorType: ConnectionLifetimeClosedErrorType,
		Cause:     errors.Join(driver.ErrBadConn, closeErr),
	}
}

// NewConnectionLifetimeFailedError reports that replacing an expired physical connection failed. It wraps
// driver.ErrBadConn for the same reason as NewConnectionLifetimeClosedError, together with the reconnect error.
func NewConnectionLifetimeFailedError(reconnectErr error) *AwsWrapperError {
	return &AwsWrapperError{
		Message:   GetMessage("ConnectionLifetimePlugin.unableToReconnect", reconnectErr.Error()),
		ErrorType: ConnectionLifetimeFailedErrorType,
		Cause:     errors.Join(driver.ErrBadConn, reconnectErr),
	}
}
