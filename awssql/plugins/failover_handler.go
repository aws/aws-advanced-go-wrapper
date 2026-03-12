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

package plugins

// FailoverHandler defines the behvaiour of failover-specific logic
type FailoverHandler interface {
	// initFailoverMode initializes the failover mode based on connection properties and URL type.
	initFailoverMode() error
	// failover performs the actual failover procedure.
	failover() error
	// dealWithError inspects an error and triggers failover if appropriate.
	dealWithError(err error) error
}
