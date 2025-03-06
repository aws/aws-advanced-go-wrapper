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

package driver

type AwsWrapperErrorType int

const (
	UnknownErrorType                      AwsWrapperErrorType = 0
	UnsupportedStrategyErrorType          AwsWrapperErrorType = 1
	UnsupportedMethodErrorType            AwsWrapperErrorType = 2
	IllegalArgumentErrorType              AwsWrapperErrorType = 3
	FailoverSuccessErrorType              AwsWrapperErrorType = 4
	FailoverFailedErrorType               AwsWrapperErrorType = 5
	TransactionResolutionUnknownErrorType AwsWrapperErrorType = 6
	LoginErrorType                        AwsWrapperErrorType = 7
	InternalQueryTimeoutErrorType         AwsWrapperErrorType = 8
	UnavailableHostErrorType              AwsWrapperErrorType = 9
	DsnParsingErrorType                   AwsWrapperErrorType = 10
)

type AwsWrapperError struct {
	message   string
	errorType AwsWrapperErrorType
}

func (a *AwsWrapperError) Error() string {
	return a.message
}

func (a *AwsWrapperError) IsType(errorType AwsWrapperErrorType) bool {
	return a.errorType == errorType
}

func (a *AwsWrapperError) IsFailoverErrorType() bool {
	return a.errorType == FailoverSuccessErrorType || a.errorType == FailoverFailedErrorType || a.errorType == TransactionResolutionUnknownErrorType
}

func NewUnsupportedStrategyError(strategy string, connectionProvider string) *AwsWrapperError {
	return &AwsWrapperError{GetMessage("ConnectionProvider.unsupportedHostSelectorStrategy", strategy, connectionProvider), UnsupportedStrategyErrorType}
}

func NewUnsupportedMethodError(methodName string) *AwsWrapperError {
	return &AwsWrapperError{GetMessage("Conn.unsupportedMethodError", methodName), UnsupportedStrategyErrorType}
}

func NewIllegalArgumentError(message string) *AwsWrapperError {
	return &AwsWrapperError{message, IllegalArgumentErrorType}
}

var FailoverSuccessError = &AwsWrapperError{GetMessage("Failover.connectionChangedError"), FailoverSuccessErrorType}

func NewFailoverFailedError(message string) *AwsWrapperError {
	return &AwsWrapperError{message, FailoverFailedErrorType}
}

var TransactionResolutionUnknownError = &AwsWrapperError{GetMessage("Failover.transactionResolutionUnknownError"), TransactionResolutionUnknownErrorType}

func NewLoginError(message string) *AwsWrapperError {
	return &AwsWrapperError{message, LoginErrorType}
}

var InternalQueryTimeoutError = &AwsWrapperError{GetMessage("Failover.timeoutError"), InternalQueryTimeoutErrorType}

func NewUnavailableHostError(host string) *AwsWrapperError {
	return &AwsWrapperError{GetMessage("HostMonitoringConnectionPlugin.unavailableHost", host), UnavailableHostErrorType}
}

func NewDsnParsingError(message string) *AwsWrapperError {
	return &AwsWrapperError{message, DsnParsingErrorType}
}
