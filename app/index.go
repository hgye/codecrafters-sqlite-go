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


