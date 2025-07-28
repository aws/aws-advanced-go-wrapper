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

package driver_infrastructure

import (
	"database/sql/driver"
	"sync"
)

var sessionStateTransferLock sync.RWMutex
var resetSessionStateOnCloseFunc func(sessionState SessionState, conn driver.Conn) bool
var transferSessionStateOnCloseFunc func(sessionState SessionState, conn driver.Conn) bool

func SetResetSessionStateOnCloseFunc(function func(sessionState SessionState, conn driver.Conn) bool) {
	sessionStateTransferLock.Lock()
	defer sessionStateTransferLock.Unlock()
	resetSessionStateOnCloseFunc = function
}

func GetResetSessionStateOnCloseFunc() func(sessionState SessionState, conn driver.Conn) bool {
	sessionStateTransferLock.RLock()
	defer sessionStateTransferLock.RUnlock()
	return resetSessionStateOnCloseFunc
}

func ClearResetSessionStateOnCloseFunc() {
	sessionStateTransferLock.Lock()
	defer sessionStateTransferLock.Unlock()
	resetSessionStateOnCloseFunc = nil
}

func SetTransferSessionStateOnCloseFunc(function func(sessionState SessionState, conn driver.Conn) bool) {
	sessionStateTransferLock.Lock()
	defer sessionStateTransferLock.Unlock()
	transferSessionStateOnCloseFunc = function
}

func GetTransferSessionStateOnCloseFunc() func(sessionState SessionState, conn driver.Conn) bool {
	sessionStateTransferLock.RLock()
	defer sessionStateTransferLock.RUnlock()
	return transferSessionStateOnCloseFunc
}

func ClearTransferSessionStateOnCloseFunc() {
	sessionStateTransferLock.Lock()
	defer sessionStateTransferLock.Unlock()
	transferSessionStateOnCloseFunc = nil
}
