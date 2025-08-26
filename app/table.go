package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
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
	for i, cellWithPos := range cells {
		row, err := t.cellToRow(&cellWithPos)
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
	cellsWithPos, err := t.tableRaw.ReadAllCells(ctx)
	if err != nil {
		return 0, fmt.Errorf("count rows for table %s: %w", t.schema.Name, err)
	}

	return len(cellsWithPos), nil
}

// GetName returns the table name
func (t *TableImpl) GetName() string {
	return t.schema.Name
}

// cellToRow converts a positioned cell to a Row, with auto-incremental 'id' as first column if needed
func (t *TableImpl) cellToRow(cellWithPos *CellWithPosition) (*Row, error) {
	cell := cellWithPos.Cell

	// Get the schema to check if we need to add an implicit id column
	schema, err := t.GetSchema(context.Background())
	if err != nil {
		return nil, fmt.Errorf("get schema for cellToRow: %w", err)
	}

	// Check if the table has an INTEGER PRIMARY KEY AUTOINCREMENT column (implicit rowid)
	hasImplicitRowid := false
	for _, col := range schema {
		// Look for INTEGER type column that would be using rowid
		// In our schema parsing, these are the actual columns from CREATE TABLE
		if strings.ToLower(col.Name) == "id" && strings.ToLower(col.Type) == "integer" {
			// This is likely an INTEGER PRIMARY KEY AUTOINCREMENT column
			hasImplicitRowid = true
			break
		}
	}

	if hasImplicitRowid {
		// Table has INTEGER PRIMARY KEY AUTOINCREMENT - use actual SQLite rowid values
		values := make([]Value, len(cell.Record.RecordBody.Values))

		// Check if the first column is the integer primary key (which should use rowid)
		useRowidForFirstColumn := len(schema) > 0 &&
			strings.ToLower(schema[0].Name) == "id" &&
			strings.ToLower(schema[0].Type) == "integer"

		for i, rawValue := range cell.Record.RecordBody.Values {
			// Get the serial type for this value from the record header
			var serialType uint64 = 0 // Default to NULL
			if i < len(cell.Record.RecordHeader.SerialTypes) {
				serialType = cell.Record.RecordHeader.SerialTypes[i]
			}

			// Create SQLiteValue based on the raw data and serial type
			var data []byte

			// For the first column, if it's an integer id column, use the rowid
			if i == 0 && useRowidForFirstColumn {
				// Use the actual rowid from the cell
				idBytes := make([]byte, 8)
				binary.BigEndian.PutUint64(idBytes, cell.Rowid)
				values[i] = NewSQLiteValue(SerialTypeInt64, idBytes)
			} else {
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
		}

		return &Row{Values: values}, nil
	} else {
		// Table does NOT have INTEGER PRIMARY KEY AUTOINCREMENT - add synthetic auto-increment ID
		// Calculate auto-incremental ID based on position in pointer array
		// This creates a 1-based auto-increment: first row = startRowId + 0 = 1, second = startRowId + 1 = 2, etc.
		autoIncrementId := cellWithPos.StartRowId + uint64(cellWithPos.Position)

		// Create values array with 'id' as first column
		values := make([]Value, len(cell.Record.RecordBody.Values)+1)

		// First value is the auto-incremental 'id'
		// Convert uint64 to big-endian byte array for SerialTypeInt64
		idBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(idBytes, autoIncrementId)
		values[0] = NewSQLiteValue(SerialTypeInt64, idBytes)

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

			// Adjust index to account for 'id' column being first
			values[i+1] = NewSQLiteValue(serialType, data)
		}

		return &Row{Values: values}, nil
	}
}

// isPrintableText checks if bytes represent printable text
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
