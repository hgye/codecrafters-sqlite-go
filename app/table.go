package main

import (
	"context"
	"fmt"
)

// TableImpl implements TableInterface
type TableImpl struct {
	tableRaw TableRaw
	schema   *SchemaRecord
	columns  []Column // cached column information
}

// NewTable creates a new logical table instance
func NewTable(tableRaw TableRaw, schema *SchemaRecord) *TableImpl {
	return &TableImpl{
		tableRaw: tableRaw,
		schema:   schema,
	}
}

// GetSchema returns the column schema for the table
func (t *TableImpl) GetSchema(ctx context.Context) ([]Column, error) {
	// Return cached columns if available
	if len(t.columns) > 0 {
		return t.columns, nil
	}

	// Parse schema from SQL
	columns, err := parseTableSchema(t.schema.SQL)
	if err != nil {
		return nil, fmt.Errorf("get table schema for %s: %w", t.schema.Name, err)
	}

	// Cache columns
	t.columns = columns
	return columns, nil
}

// GetRows returns all rows from the table
func (t *TableImpl) GetRows(ctx context.Context) ([]Row, error) {
	cells, err := t.tableRaw.ReadAllCells(ctx)
	if err != nil {
		return nil, fmt.Errorf("get rows for table %s: %w", t.schema.Name, err)
	}

	rows := make([]Row, len(cells))
	for i, cell := range cells {
		row, err := t.cellToRow(cell)
		if err != nil {
			return nil, fmt.Errorf("convert cell %d to row for table %s: %w",
				i, t.schema.Name, err)
		}
		rows[i] = *row
	}

	return rows, nil
}

// SelectColumns returns rows with only the specified columns
func (t *TableImpl) SelectColumns(ctx context.Context, columns []string) ([]Row, error) {
	// Get all rows first
	allRows, err := t.GetRows(ctx)
	if err != nil {
		return nil, err
	}

	// Get column schema to map column names to indices
	schema, err := t.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	// Create column index mapping
	columnIndices := make([]int, len(columns))
	for i, colName := range columns {
		found := false
		for j, schemaCol := range schema {
			if schemaCol.Name == colName {
				columnIndices[i] = j
				found = true
				break
			}
		}
		if !found {
			return nil, NewDatabaseError("select_columns", ErrColumnNotFound, map[string]interface{}{
				"table_name":  t.schema.Name,
				"column_name": colName,
			})
		}
	}

	// Filter rows to include only selected columns
	filteredRows := make([]Row, len(allRows))
	for i, row := range allRows {
		filteredValues := make([]Value, len(columnIndices))
		for j, colIndex := range columnIndices {
			if colIndex < len(row.Values) {
				filteredValues[j] = row.Values[colIndex]
			} else {
				filteredValues[j] = NewSQLiteValue(0, nil) // NULL value
			}
		}
		filteredRows[i] = Row{
			Values: filteredValues,
		}
	}

	return filteredRows, nil
}

// Filter returns rows that match the given condition
func (t *TableImpl) Filter(ctx context.Context, condition func(Row) bool) ([]Row, error) {
	allRows, err := t.GetRows(ctx)
	if err != nil {
		return nil, err
	}

	var filteredRows []Row
	for _, row := range allRows {
		if condition(row) {
			filteredRows = append(filteredRows, row)
		}
	}

	return filteredRows, nil
}

// Count returns the number of rows in the table
func (t *TableImpl) Count(ctx context.Context) (int, error) {
	cells, err := t.tableRaw.ReadAllCells(ctx)
	if err != nil {
		return 0, fmt.Errorf("count rows for table %s: %w", t.schema.Name, err)
	}

	return len(cells), nil
}

// GetName returns the table name
func (t *TableImpl) GetName() string {
	return t.schema.Name
}

// cellToRow converts a cell to a row
func (t *TableImpl) cellToRow(cell Cell) (*Row, error) {
	values := make([]Value, len(cell.Record.RecordBody.Values))

	for i, rawValue := range cell.Record.RecordBody.Values {
		// Get the serial type for this value from the record header
		var serialType uint64 = 0 // Default to NULL
		if i < len(cell.Record.RecordHeader.SerialTypes) {
			serialType = cell.Record.RecordHeader.SerialTypes[i]
		}

		// Create SQLiteValue based on the raw data and serial type
		var data []byte
		if rawValue != nil {
			if bytes, ok := rawValue.([]byte); ok {
				data = bytes
			} else {
				// Convert other types to bytes if needed
				data = []byte(fmt.Sprintf("%v", rawValue))
			}
		}

		values[i] = NewSQLiteValue(serialType, data)
	}

	return &Row{
		Values: values,
	}, nil
} // isPrintableText checks if bytes represent printable text
func isPrintableText(data []byte) bool {
	for _, b := range data {
		if b < 32 && b != 9 && b != 10 && b != 13 { // Allow tab, newline, carriage return
			return false
		}
		if b > 126 {
			return false
		}
	}
	return true
}
