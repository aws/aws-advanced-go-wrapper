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

package property_util

import (
	"regexp"

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

var httpsUrlPattern = regexp.MustCompile(`^(https)://[-a-zA-Z0-9+&@#/%?=~_!:,.']*[-a-zA-Z0-9+&@#/%=~_']`)

func CheckIdpCredentialsWithFallback(
	idpUserNameProperty AwsWrapperProperty,
	idpPasswordProperty AwsWrapperProperty,
	props *utils.RWMap[string]) {
	_, ok := props.Get(idpUserNameProperty.Name)
	if !ok {
		idpUserNameProperty.Set(props, GetVerifiedWrapperPropertyValue[string](props, USER))
	}

	_, ok = props.Get(idpPasswordProperty.Name)
	if !ok {
		idpPasswordProperty.Set(props, GetVerifiedWrapperPropertyValue[string](props, PASSWORD))
	}
}

func ValidateUrl(url string) error {
	match := httpsUrlPattern.MatchString(url)
	if !match {
		return error_util.NewGenericAwsWrapperError(error_util.GetMessage("AdfsCredentialsProviderFactory.invalidHttpsUrl", url))
	}
	return nil
}
