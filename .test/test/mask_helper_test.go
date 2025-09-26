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
	"strings"
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/stretchr/testify/assert"
)

func TestMaskProperties_WithSensitiveProps(t *testing.T) {
	props := MakeMapFromKeysAndVals(
		property_util.PASSWORD.Name, "DoNotShow",
		property_util.IDP_PASSWORD.Name, "AlsoDoNotShow",
		property_util.USER.Name, "username",
	)
	maskedProps := property_util.MaskProperties(props)
	assert.Equal(t, "***", maskedProps[property_util.PASSWORD.Name])
	assert.Equal(t, "***", maskedProps[property_util.IDP_PASSWORD.Name])
	assert.Equal(t, "username", maskedProps[property_util.USER.Name])
}

func TestMaskProperties_WithNoSensitiveProps(t *testing.T) {
	props := MakeMapFromKeysAndVals(property_util.USER.Name, "username")

	maskedProps := property_util.MaskProperties(props)
	assert.Equal(t, "username", maskedProps[property_util.USER.Name])
}

func TestMaskProperties_WithEmptyProps(t *testing.T) {
	props := utils.NewRWMap[string]()
	maskedProps := property_util.MaskProperties(props)
	assert.Equal(t, 0, len(maskedProps))
}

func TestMaskSensitiveInfoFromDsn_PgxUrl(t *testing.T) {
	dsnWithPass := "postgres://someUser:somePassword@mydatabase.cluster-xyz.us-east-2.rds.amazonaws.com:5432/pgx_test?sslmode=disable&foo=bar&idpPassword=myIdpPassword"

	maskedDsn := property_util.MaskSensitiveInfoFromDsn(dsnWithPass)

	// Cannot check with string because order is not guaranteed with Url.
	assert.True(t, strings.Contains(maskedDsn, "someUser:***"))
	assert.True(t, strings.Contains(maskedDsn, "idpPassword=***"))
	assert.False(t, strings.Contains(maskedDsn, "somePassword"))
	assert.False(t, strings.Contains(maskedDsn, "myIdpPassword"))

	dsnWithNoPassWithIdpPassword := "postgres://@mydatabase.cluster-xyz.us-east-2.rds.amazonaws.com:5432/pgx_test?sslmode=disable&foo=bar&idpPassword=myIdpPassword"
	maskedDsn = property_util.MaskSensitiveInfoFromDsn(dsnWithNoPassWithIdpPassword)
	assert.False(t, strings.Contains(maskedDsn, "myIdpPassword"))

	dsnWithNoSensitiveInfo := "postgres://@mydatabase.cluster-xyz.us-east-2.rds.amazonaws.com:5432/pgx_test?sslmode=disable&foo=bar"
	maskedDsn = property_util.MaskSensitiveInfoFromDsn(dsnWithNoSensitiveInfo)
	assert.Equal(t, len(dsnWithNoSensitiveInfo), len(maskedDsn))
}

func TestMaskSensitiveInfoFromDsn_MySQL(t *testing.T) {
	dsnWithPass := "someUser:somePassword@tcp(mydatabase.cluster-xyz.us-east-2.rds.amazonaws.com:3306)/myDatabase?foo=bar&pop=snap&idpPassword=myIdpPassword"

	maskedDsn := property_util.MaskSensitiveInfoFromDsn(dsnWithPass)

	// Cannot check with string because order is not guaranteed with Url.
	assert.True(t, strings.Contains(maskedDsn, "someUser:***"))
	assert.True(t, strings.Contains(maskedDsn, "idpPassword=***"))
	assert.False(t, strings.Contains(maskedDsn, "somePassword"))
	assert.False(t, strings.Contains(maskedDsn, "myIdpPassword"))

	dsnWithNoPassWithIdpPassword := "@tcp(mydatabase.cluster-xyz.us-east-2.rds.amazonaws.com:3306)/myDatabase?foo=bar&pop=snap&idpPassword=myIdpPassword"
	maskedDsn = property_util.MaskSensitiveInfoFromDsn(dsnWithNoPassWithIdpPassword)
	assert.False(t, strings.Contains(maskedDsn, "myIdpPassword"))

	dsnWithNoSensitiveInfo := "@tcp(mydatabase.cluster-xyz.us-east-2.rds.amazonaws.com:3306)/myDatabase?foo=bar&pop=snap"
	maskedDsn = property_util.MaskSensitiveInfoFromDsn(dsnWithNoSensitiveInfo)
	assert.Equal(t, dsnWithNoSensitiveInfo, maskedDsn)
}

func TestMaskSensitiveInfoFromDsn_PgxKeywordValue(t *testing.T) {
	dsnWithPass := "host=myHost port=5432 user=user password=somePassword dbname=db idpPassword=myIdpPassword"

	expectedMaskedDsnWithPass := "host=myHost port=5432 user=user password=*** dbname=db idpPassword=***"
	maskedDsn := property_util.MaskSensitiveInfoFromDsn(dsnWithPass)

	assert.Equal(t, expectedMaskedDsnWithPass, maskedDsn)

	dsnWithNoPassWithIdpPassword := "host=myHost port=5432 user=user dbname=db idpPassword=myIdpPassword"
	expectedMaskedDsnWithNoPassWithIdpPassword := "host=myHost port=5432 user=user dbname=db idpPassword=***"
	maskedDsn = property_util.MaskSensitiveInfoFromDsn(dsnWithNoPassWithIdpPassword)
	assert.Equal(t, expectedMaskedDsnWithNoPassWithIdpPassword, maskedDsn)

	dsnWithNoSensitiveInfo := "host=myHost port=5432 user=user dbname=db"
	maskedDsn = property_util.MaskSensitiveInfoFromDsn(dsnWithNoSensitiveInfo)
	assert.Equal(t, dsnWithNoSensitiveInfo, maskedDsn)
}
