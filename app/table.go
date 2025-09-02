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
	indexes  []Index  // cached indexes for this table
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

// GetIndexes returns all indexes associated with this table
func (t *TableImpl) GetIndexes(ctx context.Context) ([]Index, error) {
	return t.indexes, nil
}

// AddIndex adds an index to this table's cached indexes
func (t *TableImpl) AddIndex(index Index) {
	t.indexes = append(t.indexes, index)
}

// GetIndexByName returns a specific index by name
func (t *TableImpl) GetIndexByName(name string) (Index, bool) {
	for _, index := range t.indexes {
		if index.GetName() == name {
			return index, true
		}
	}
	return nil, false
}

// GetRowByRowid gets a specific row by its rowid
func (t *TableImpl) GetRowByRowid(ctx context.Context, rowid int64) (*Row, error) {
	// Use the raw table to get the specific cell
	cell, err := t.tableRaw.ReadCellByRowid(ctx, rowid)
	if err != nil {
		return nil, fmt.Errorf("get cell for rowid %d in table %s: %w", rowid, t.schema.Name, err)
	}

	// Convert the cell to a row
	row, err := t.cellToRow(*cell)
	if err != nil {
		return nil, fmt.Errorf("convert cell to row for rowid %d in table %s: %w", rowid, t.schema.Name, err)
	}

	return row, nil
}

// cellToRow converts a SQLite cell to a logical row
//
// SQLite Record Format:
// - Record Header: Contains serial types for each column in schema order (serial type 0 = NULL or not stored)
// - Record Body: Contains actual data values in order (only for columns with serial type != 0)
// - INTEGER PRIMARY KEY AUTOINCREMENT columns typically have serial type 0 (use rowid instead)
func (t *TableImpl) cellToRow(cell Cell) (*Row, error) {
	columns, err := t.GetSchema(context.Background())
	if err != nil {
		return nil, fmt.Errorf("get schema for cellToRow: %w", err)
	}

	autoincrementColumnIndex := t.findAutoIncrementColumnIndex(columns)
	values := make([]Value, len(columns))

	processor := &columnProcessor{
		cell:                     cell,
		recordBodyIndex:          0,
		autoincrementColumnIndex: autoincrementColumnIndex,
	}

	for i := 0; i < len(columns); i++ {
		serialType := processor.getSerialType(i)
		values[i] = processor.processColumn(i, serialType)
	}

	return &Row{Values: values}, nil
}

// findAutoIncrementColumnIndex finds the index of the auto-increment primary key column
func (t *TableImpl) findAutoIncrementColumnIndex(columns []Column) int {
	for i, col := range columns {
		if col.IsPrimaryKey && col.IsAutoIncrement {
			return i
		}
	}
	return -1
}

// columnProcessor handles the processing of individual columns during row conversion
type columnProcessor struct {
	cell                     Cell
	recordBodyIndex          int
	autoincrementColumnIndex int
}

// getSerialType returns the serial type for the given column index
func (cp *columnProcessor) getSerialType(columnIndex int) uint64 {
	if columnIndex < len(cp.cell.Record.RecordHeader.SerialTypes) {
		return cp.cell.Record.RecordHeader.SerialTypes[columnIndex]
	}
	return 0
}

// processColumn processes a single column and returns its value
func (cp *columnProcessor) processColumn(columnIndex int, serialType uint64) Value {
	switch {
	case serialType == 0 && columnIndex == cp.autoincrementColumnIndex:
		return cp.handleAutoIncrementColumn()
	case serialType == 0:
		return cp.handleNullColumn()
	default:
		return cp.handleRegularColumn(serialType)
	}
}

// handleAutoIncrementColumn creates a value from the cell's rowid for auto-increment columns
func (cp *columnProcessor) handleAutoIncrementColumn() Value {
	rowidString := fmt.Sprintf("%d", cp.cell.Rowid)
	textSerialType := uint64(13 + 2*len(rowidString)) // Text serial type formula
	return NewSQLiteValue(textSerialType, []byte(rowidString))
}

// handleNullColumn creates a NULL value for columns with no stored data
func (cp *columnProcessor) handleNullColumn() Value {
	return NewSQLiteValue(0, nil)
}

// handleRegularColumn reads and creates a value from the record body for regular columns
func (cp *columnProcessor) handleRegularColumn(serialType uint64) Value {
	if cp.recordBodyIndex >= len(cp.cell.Record.RecordBody.Values) {
		return NewSQLiteValue(0, nil) // No more data available
	}

	rawValue := cp.cell.Record.RecordBody.Values[cp.recordBodyIndex]
	cp.recordBodyIndex++ // Consume data from record body

	data := cp.convertToBytes(rawValue)
	return NewSQLiteValue(serialType, data)
}

// convertToBytes converts raw value to byte slice for storage
func (cp *columnProcessor) convertToBytes(rawValue interface{}) []byte {
	if rawValue == nil {
		return nil
	}

	if bytes, ok := rawValue.([]byte); ok {
		return bytes
	}

	return []byte(fmt.Sprintf("%v", rawValue))
}

// isPrintableText checks if bytes represent printable text
// func isPrintableText(data []byte) bool {
// 	for _, b := range data {
// 		if b < 32 && b != 9 && b != 10 && b != 13 { // Allow tab, newline, carriage return
// 			return false
// 		}
// 		if b > 126 {
// 			return false
// 		}
// 	}
// 	return true
// }
