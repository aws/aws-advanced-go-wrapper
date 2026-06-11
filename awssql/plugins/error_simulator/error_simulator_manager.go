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

package error_simulator

import (
	"sync"
)

var (
	instance *ErrorSimulatorManager
	once     sync.Once
)

type ErrorSimulatorManager struct {
	mu              sync.Mutex
	NextError       error
	ConnectCallback ErrorSimulatorConnectCallback
}

func (e *ErrorSimulatorManager) RaiseErrorOnNextConnect(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.NextError = err
}
func (e *ErrorSimulatorManager) SetCallback(errorSimulatorConnectCallback ErrorSimulatorConnectCallback) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ConnectCallback = errorSimulatorConnectCallback
}

func GetErrorSimulatorManager() *ErrorSimulatorManager {
	once.Do(func() {
		instance = &ErrorSimulatorManager{}
	})
	return instance
}

func ResetErrorSimulatorManager() {
	m := GetErrorSimulatorManager()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.NextError = nil
	m.ConnectCallback = nil
}

func (e *ErrorSimulatorManager) GetNextError() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.NextError
}

func (e *ErrorSimulatorManager) ConsumeNextError() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.NextError = nil
}

func (e *ErrorSimulatorManager) GetConnectCallback() ErrorSimulatorConnectCallback {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.ConnectCallback
}
