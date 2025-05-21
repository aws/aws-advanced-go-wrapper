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

package test_utils

import (
	"awssql/error_util"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/xuri/excelize/v2"
)

func WritePerfDataToFile(data []PerfStat, fileName string, sheetName string) error {
	if len(data) == 0 {
		return fmt.Errorf("no data to write")
	}

	// Ensure the "reports" directory exists
	reportDir := "reports"
	if err := os.MkdirAll(reportDir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create reports directory: %v", err)
	}

	// Prepend the directory to the file name
	fullPath := filepath.Join(reportDir, fileName)

	f := excelize.NewFile()

	// Create a new sheet
	index, err := f.NewSheet(sheetName)

	if err != nil {
		return error_util.NewGenericAwsWrapperError(fmt.Sprintf("failed to create sheet: %v", err))
	}

	// Delete the default "Sheet1"
	err = f.DeleteSheet("Sheet1")

	if err != nil {
		return error_util.NewGenericAwsWrapperError(fmt.Sprintf("Could not delete default sheet %v", err))
	}

	headers := data[0].WriteHeader()
	for colIdx, header := range headers {
		cellRef, _ := excelize.CoordinatesToCellName(colIdx+1, 1)
		err = f.SetCellValue(sheetName, cellRef, header)

		if err != nil {
			slog.Debug(
				fmt.Sprintf("Could not set cell value for sheet '%v', in cell '%v', in header '%v'",
					sheetName,
					cellRef,
					header))
		}
	}

	for rowIdx, entry := range data {
		values := entry.WriteData()
		for colIdx, value := range values {
			cellRef, err := excelize.CoordinatesToCellName(colIdx+1, rowIdx+2)
			if err != nil {
				return fmt.Errorf("invalid cell coordinates: %v", err)
			}
			if err := f.SetCellValue(sheetName, cellRef, value); err != nil {
				return fmt.Errorf("failed to set cell value: %v", err)
			}
		}
	}
	f.SetActiveSheet(index)

	if err := f.SaveAs(fullPath); err != nil {
		return fmt.Errorf("failed to save file: %v", err)
	}
	return nil
}
