package main

import (
	"context"
	"fmt"
)

// IndexImpl implements Index interface
type IndexImpl struct {
	indexRaw  IndexRaw
	schema    *SchemaRecord
	tableName string // name of the table this index belongs to
}

// NewIndex creates a new logical index instance
func NewIndex(indexRaw IndexRaw, schema *SchemaRecord) *IndexImpl {
	return &IndexImpl{
		indexRaw:  indexRaw,
		schema:    schema,
		tableName: schema.TblName, // Set the table name from schema
	}
}

// GetSchema returns the column schema for the index
func (i *IndexImpl) GetSchema(ctx context.Context) ([]Column, error) {
	// For indices, we need to parse the indexed columns from the CREATE INDEX SQL
	columns := make([]Column, len(i.indexRaw.GetIndexedColumns()))
	for idx, colName := range i.indexRaw.GetIndexedColumns() {
		columns[idx] = Column{
			Name:  colName,
			Type:  "TEXT", // Default to TEXT for simplicity
			Index: idx,
		}
	}
	return columns, nil
}

// GetRows returns all index entries as rows
func (i *IndexImpl) GetRows(ctx context.Context) ([]Row, error) {
	cells, err := i.indexRaw.ReadAllCells(ctx)
	if err != nil {
		return nil, fmt.Errorf("get rows for index %s: %w", i.schema.Name, err)
	}

	rows := make([]Row, len(cells))
	for idx, cell := range cells {
		row, err := i.cellToRow(cell)
		if err != nil {
			return nil, fmt.Errorf("convert cell %d to row for index %s: %w",
				idx, i.schema.Name, err)
		}
		rows[idx] = *row
	}

	return rows, nil
}

// Count returns the number of entries in the index
func (i *IndexImpl) Count(ctx context.Context) (int, error) {
	cells, err := i.indexRaw.ReadAllCells(ctx)
	if err != nil {
		return 0, fmt.Errorf("count rows for index %s: %w", i.schema.Name, err)
	}

	return len(cells), nil
}

// GetName returns the index name
func (i *IndexImpl) GetName() string {
	return i.schema.Name
}

// GetTableName returns the name of the table this index belongs to
func (i *IndexImpl) GetTableName() string {
	return i.tableName
}

// SearchByKey searches the index for entries with the specified key value
func (i *IndexImpl) SearchByKey(ctx context.Context, key interface{}) ([]IndexEntry, error) {
	return i.indexRaw.SearchKeys(ctx, key)
}

// cellToRow converts a SQLite cell to a logical row for index display
func (i *IndexImpl) cellToRow(cell Cell) (*Row, error) {
	// For indices, we show the indexed column values plus the rowid
	indexedColumns := i.indexRaw.GetIndexedColumns()
	values := make([]Value, len(indexedColumns)+1) // +1 for rowid

	// Extract values from record body
	for idx, rawValue := range cell.Record.RecordBody.Values {
		if idx >= len(indexedColumns) {
			break
		}

		// Convert raw value to SQLite value
		var serialType uint64 = 1 // Default to integer
		if idx < len(cell.Record.RecordHeader.SerialTypes) {
			serialType = cell.Record.RecordHeader.SerialTypes[idx]
		}

		data := i.convertToBytes(rawValue)
		values[idx] = NewSQLiteValue(serialType, data)
	}

	// Add rowid as the last column
	rowidString := fmt.Sprintf("%d", cell.Rowid)
	textSerialType := uint64(13 + 2*len(rowidString)) // Text serial type formula
	values[len(indexedColumns)] = NewSQLiteValue(textSerialType, []byte(rowidString))

	return &Row{Values: values}, nil
}

// convertToBytes converts raw value to byte slice for storage
func (i *IndexImpl) convertToBytes(rawValue interface{}) []byte {
	if rawValue == nil {
		return nil
	}

	if bytes, ok := rawValue.([]byte); ok {
		return bytes
	}

	return []byte(fmt.Sprintf("%v", rawValue))
}
