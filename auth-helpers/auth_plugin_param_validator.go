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

package auth_helpers

import (
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
)

func ValidateAuthParams(pluginCode string,
	dbUser string,
	idpUsername string,
	idpPassword string,
	idpEndpoint string,
	iamRoleArn string,
	iamIdpArn string,
	appId string) error {
	var appIdRequiredPluginCodes = map[string]struct{}{
		"okta": {},
	}

	var missingMandatoryParams []string

	if dbUser == "" {
		missingMandatoryParams = append(missingMandatoryParams, property_util.DB_USER.Name)
	}
	if idpUsername == "" {
		missingMandatoryParams = append(missingMandatoryParams, property_util.IDP_USERNAME.Name)
	}
	if idpPassword == "" {
		missingMandatoryParams = append(missingMandatoryParams, property_util.IDP_PASSWORD.Name)
	}
	if idpEndpoint == "" {
		missingMandatoryParams = append(missingMandatoryParams, property_util.IDP_ENDPOINT.Name)
	}
	if iamRoleArn == "" {
		missingMandatoryParams = append(missingMandatoryParams, property_util.IAM_ROLE_ARN.Name)
	}
	if iamIdpArn == "" {
		missingMandatoryParams = append(missingMandatoryParams, property_util.IAM_IDP_ARN.Name)
	}
	if _, required := appIdRequiredPluginCodes[pluginCode]; required && appId == "" {
		missingMandatoryParams = append(missingMandatoryParams, property_util.APP_ID.Name)
	}

	if len(missingMandatoryParams) > 0 {
		return error_util.NewGenericAwsWrapperError(error_util.GetMessage("AuthHelpers.missingRequiredParameters", pluginCode, missingMandatoryParams))
	}

	return nil
}
