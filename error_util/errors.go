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
	FailoverSuccessErrorType              AwsWrapperErrorType = 300
	FailoverFailedErrorType               AwsWrapperErrorType = 301
	TransactionResolutionUnknownErrorType AwsWrapperErrorType = 302
)

type AwsWrapperError struct {
	Message   string
	ErrorType AwsWrapperErrorType
}

func (a *AwsWrapperError) Error() string {
	return a.Message
}

func (a *AwsWrapperError) IsType(errorType AwsWrapperErrorType) bool {
	return a.ErrorType == errorType
}

func (a *AwsWrapperError) IsFailoverErrorType() bool {
	return a.ErrorType >= 300
}

func NewGenericAwsWrapperError(message string) *AwsWrapperError {
	return &AwsWrapperError{message, GenericAwsWrapperErrorType}
}

func NewUnsupportedStrategyError(message string) *AwsWrapperError {
	return &AwsWrapperError{
		message,
		UnsupportedStrategyErrorType}
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

var TransactionResolutionUnknownError = &AwsWrapperError{
	GetMessage("Failover.transactionResolutionUnknownError"),
	TransactionResolutionUnknownErrorType}

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

var ShouldNotBeCalledError = &AwsWrapperError{Message: "Shouldn't be called.", ErrorType: GenericAwsWrapperErrorType}
