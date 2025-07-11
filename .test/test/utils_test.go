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
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/stretchr/testify/assert"
)

func TestRollbackWithCurrentTx(t *testing.T) {
	mockConn := MockConn{}
	mockTx := NewMockTx()

	utils.Rollback(&mockConn, mockTx)
	assert.Equal(t, 1, *mockTx.rollbackCounter)
	assert.Equal(t, 0, mockConn.execContextCounter)
}

func TestRollbackWithNilCurrentTx(t *testing.T) {
	mockConn := MockConn{}

	utils.Rollback(&mockConn, nil)
	assert.Equal(t, 1, mockConn.execContextCounter)
}

func TestCombineMaps(t *testing.T) {
	map1 := map[int]int{}
	map2 := map[int]int{}
	assert.Equal(t, map[int]int{}, utils.CombineMaps(map1, map2))

	map3 := map[int]string{
		1: "one",
		2: "two",
	}
	map4 := map[int]string{
		3: "three",
		4: "four",
	}
	assert.Equal(t,
		map[int]string{
			1: "one",
			2: "two",
			3: "three",
			4: "four",
		},
		utils.CombineMaps(map3, map4))

	map5 := map[string]int{
		"five":  5,
		"three": 3,
	}
	map6 := map[string]int{
		"three": 33,
		"two":   2,
	}
	assert.Equal(t,
		map[string]int{
			"five":  5,
			"three": 33,
			"two":   2,
		},
		utils.CombineMaps(map5, map6))
}
