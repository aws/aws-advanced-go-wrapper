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

package iam

import "time"

type TokenInfo struct {
	token      string
	expiration time.Time
}

func NewTokenInfo(token string, expiration time.Time) TokenInfo {
	return TokenInfo{token, expiration}
}

func (t TokenInfo) GetToken() string {
	return t.token
}

func (t TokenInfo) IsExpired() bool {
	return t.expiration.Before(time.Now())
}
