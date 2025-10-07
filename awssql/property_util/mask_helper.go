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
	"net/url"
	"regexp"
	"strings"

	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

var SENSITIVE_PROPERTIES = map[string]struct{}{
	PASSWORD.Name:     {},
	IDP_PASSWORD.Name: {},
}

func MaskProperties(props *utils.RWMap[string, string]) map[string]string {
	maskedProps := props.GetAllEntries()

	for key, property := range maskedProps {
		if _, exists := SENSITIVE_PROPERTIES[key]; exists {
			maskedProps[key] = "***"
		} else {
			maskedProps[key] = property
		}
	}

	return maskedProps
}

func MaskSensitiveInfoFromDsn(dsn string) string {
	if isDsnPgxUrl(dsn) {
		return maskSensitiveInfoFromPgxUrl(dsn)
	}

	if isDsnMySql(dsn) {
		return maskSensitiveInfoFromMySQL(dsn)
	}

	if isDsnPgxKeyValueString(dsn) {
		return maskSensitiveInfoFromPgxKeywordValue(dsn)
	}

	return dsn
}

func maskSensitiveInfoFromPgxUrl(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return dsn
	}

	// Mask Username and Password
	if u.User != nil {
		if _, hasPassword := u.User.Password(); hasPassword {
			u.User = url.UserPassword(u.User.Username(), "***")
		}
	}

	// Mask Query params
	query := u.Query()
	for key := range SENSITIVE_PROPERTIES {
		if _, ok := query[key]; ok {
			query.Set(key, "***")
		}
	}
	u.RawQuery = query.Encode()

	// Url escapes '*' to '%2A'
	return strings.ReplaceAll(u.String(), "%2A%2A%2A", "***")
}

func maskSensitiveInfoFromMySQL(dsn string) string {
	// Masks user:password
	re := regexp.MustCompile(`(?P<user>[^:]+):(?P<password>[^@]+)@`)
	masked := re.ReplaceAllString(dsn, `${user}:***@`)

	parts := strings.SplitN(masked, "?", 2)
	if len(parts) == 2 {
		base := parts[0]
		query := parts[1]
		values, err := url.ParseQuery(query)
		if err == nil {
			changed := false
			for key := range SENSITIVE_PROPERTIES {
				if _, ok := values[key]; ok {
					values.Set(key, "***")
					changed = true
				}
			}
			if changed {
				masked = base + "?" + values.Encode()
			}
		}
	}

	// Url excapes '*' to '%2A'
	return strings.ReplaceAll(masked, "%2A%2A%2A", "***")
}

func maskSensitiveInfoFromPgxKeywordValue(dsn string) string {
	var keys []string
	for k := range SENSITIVE_PROPERTIES {
		keys = append(keys, regexp.QuoteMeta(k))
	}
	// build regex from sensitive properties
	kvRe := regexp.MustCompile(`\b(` + strings.Join(keys, "|") + `)=([^\s]+)`)
	return kvRe.ReplaceAllString(dsn, `$1=***`)
}
