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
	"database/sql/driver"
	"io"
	"testing"

	mock_database_sql_driver "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/database_sql_driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestRollbackWithCurrentTx(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockTx := mock_database_sql_driver.NewMockTx(ctrl)

	mockTx.EXPECT().Rollback().Return(nil).Times(1)

	utils.Rollback(mockConn, mockTx)
}

func TestRollbackWithNilCurrentTx(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockExecer := mock_database_sql_driver.NewMockExecerContext(ctrl)

	mockExecer.EXPECT().ExecContext(gomock.Any(), "rollback", gomock.Any()).Return(nil, nil).Times(1)

	// Create a combined mock that implements both Conn and ExecerContext
	combinedMock := struct {
		*mock_database_sql_driver.MockConn
		*mock_database_sql_driver.MockExecerContext
	}{mockConn, mockExecer}

	utils.Rollback(combinedMock, nil)
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

func TestGetFirstRowFromQuery(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Test with mock connection that returns data
	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockQueryer := mock_database_sql_driver.NewMockQueryerContext(ctrl)
	mockRows := mock_database_sql_driver.NewMockRows(ctrl)

	// Set up expectations for successful query
	mockQueryer.EXPECT().QueryContext(gomock.Any(), "SELECT * FROM test", gomock.Any()).Return(mockRows, nil).Times(1)
	mockRows.EXPECT().Columns().Return([]string{"col1", "col2", "col3"}).Times(1)
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = "value1"
		dest[1] = "value2"
		dest[2] = "value3"
		return nil
	}).Times(1)
	mockRows.EXPECT().Close().Return(nil).Times(1)

	// Create a combined mock that implements both Conn and QueryerContext
	combinedMock := struct {
		*mock_database_sql_driver.MockConn
		*mock_database_sql_driver.MockQueryerContext
	}{mockConn, mockQueryer}

	result := utils.GetFirstRowFromQuery(combinedMock, "SELECT * FROM test")
	assert.NotNil(t, result)
	assert.Equal(t, 3, len(result))
	assert.Equal(t, "value1", result[0])
	assert.Equal(t, "value2", result[1])
	assert.Equal(t, "value3", result[2])

	// Test with mock connection that returns no data
	mockConn2 := mock_database_sql_driver.NewMockConn(ctrl)
	mockQueryer2 := mock_database_sql_driver.NewMockQueryerContext(ctrl)
	mockRows2 := mock_database_sql_driver.NewMockRows(ctrl)

	mockQueryer2.EXPECT().QueryContext(gomock.Any(), "SELECT * FROM empty", gomock.Any()).Return(mockRows2, nil).Times(1)
	mockRows2.EXPECT().Columns().Return([]string{}).Times(1)
	mockRows2.EXPECT().Next(gomock.Any()).Return(io.EOF).Times(1) // Still called even with empty columns
	mockRows2.EXPECT().Close().Return(nil).Times(1)

	combinedMock2 := struct {
		*mock_database_sql_driver.MockConn
		*mock_database_sql_driver.MockQueryerContext
	}{mockConn2, mockQueryer2}

	result = utils.GetFirstRowFromQuery(combinedMock2, "SELECT * FROM empty")
	assert.Nil(t, result)

	// Test with connection that doesn't implement QueryerContext (just MockConn)
	mockConn3 := mock_database_sql_driver.NewMockConn(ctrl)
	result = utils.GetFirstRowFromQuery(mockConn3, "SELECT * FROM test")
	assert.Nil(t, result)
}

func TestFilterSliceFindFirst(t *testing.T) {
	numbers := []int{1, 3, 4, 5, 6, 7}
	result := utils.FilterSliceFindFirst(numbers, func(n int) bool {
		return n%2 == 0
	})
	assert.Equal(t, 4, result)

	words := []string{"apple", "banana", "cherry", "blueberry"}
	resultStr := utils.FilterSliceFindFirst(words, func(s string) bool {
		return len(s) > 0 && s[0] == 'b'
	})
	assert.Equal(t, "banana", resultStr)

	oddNumbers := []int{1, 3, 5, 7}
	resultZero := utils.FilterSliceFindFirst(oddNumbers, func(n int) bool {
		return n%2 == 0
	})
	assert.Equal(t, 0, resultZero)

	emptySlice := []int{}
	resultEmpty := utils.FilterSliceFindFirst(emptySlice, func(n int) bool {
		return n > 0
	})
	assert.Equal(t, 0, resultEmpty)
}

func TestFilterSetFindFirst(t *testing.T) {
	testMap := map[string]int{
		"apple":  1,
		"banana": 2,
		"cherry": 3,
		"tomato": 4,
	}
	result := utils.FilterSetFindFirst(testMap, func(key string) bool {
		return len(key) > 0 && key[0] == 't'
	})
	assert.Equal(t, "tomato", result)

	intMap := map[int]string{
		1: "one",
		2: "two",
		3: "three",
		4: "four",
	}
	resultInt := utils.FilterSetFindFirst(intMap, func(key int) bool {
		return key%2 == 0
	})
	assert.True(t, resultInt == 2 || resultInt == 4)

	resultZero := utils.FilterSetFindFirst(testMap, func(key string) bool {
		return len(key) > 0 && key[0] == 'z'
	})
	assert.Equal(t, "", resultZero)

	emptyMap := map[string]int{}
	resultEmpty := utils.FilterSetFindFirst(emptyMap, func(key string) bool {
		return len(key) > 0
	})
	assert.Equal(t, "", resultEmpty)
}

func TestFilterMapFindFirstValue(t *testing.T) {
	testMap := map[string]string{
		"fruit1": "apple",
		"fruit2": "banana",
		"fruit3": "cherry",
		"fruit4": "blueberry",
	}
	result := utils.FilterMapFindFirstValue(testMap, func(value string) bool {
		return len(value) > 0 && value[0] == 'b'
	})
	assert.True(t, result == "banana" || result == "blueberry")

	intMap := map[string]int{
		"a": 1,
		"b": 2,
		"c": 3,
		"d": 4,
	}
	resultInt := utils.FilterMapFindFirstValue(intMap, func(value int) bool {
		return value%2 == 0
	})
	assert.True(t, resultInt == 2 || resultInt == 4)

	resultZero := utils.FilterMapFindFirstValue(testMap, func(value string) bool {
		return len(value) > 0 && value[0] == 'z'
	})
	assert.Equal(t, "", resultZero)

	emptyMap := map[string]string{}
	resultEmpty := utils.FilterMapFindFirstValue(emptyMap, func(value string) bool {
		return len(value) > 0
	})
	assert.Equal(t, "", resultEmpty)
}

func TestPair(t *testing.T) {
	pair := utils.NewPair("hello", 42)
	assert.Equal(t, "hello", pair.GetLeft(), "Should store left value correctly")
	assert.Equal(t, 42, pair.GetRight(), "Should store right value correctly")

	intPair := utils.NewPair(10, 20)
	assert.Equal(t, 10, intPair.GetLeft(), "Should store left int correctly")
	assert.Equal(t, 20, intPair.GetRight(), "Should store right int correctly")
}
