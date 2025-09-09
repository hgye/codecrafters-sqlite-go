package main

import (
	"context"
	"fmt"
	"strings"
)

// IndexRawImpl implements IndexRaw interface for raw SQLite index operations
type IndexRawImpl struct {
	dbRaw          DatabaseRaw
	name           string
	rootPage       int
	indexedColumns []string // columns that this index covers
	tableName      string   // table this index belongs to
}

// NewIndexRaw creates a new raw index instance
func NewIndexRaw(dbRaw DatabaseRaw, name string, rootPage int, schema *SchemaRecord) *IndexRawImpl {
	indexedColumns := parseIndexColumns(schema.SQL)
	tableName := parseIndexTableName(schema.SQL)

	return &IndexRawImpl{
		dbRaw:          dbRaw,
		name:           name,
		rootPage:       rootPage,
		indexedColumns: indexedColumns,
		tableName:      tableName,
	}
}

// ReadAllCells reads all cells from the index B-tree using B-tree abstraction
func (ir *IndexRawImpl) ReadAllCells(ctx context.Context) ([]Cell, error) {
	// Use B-tree abstraction for traversal
	btree := NewBTree(ir.dbRaw, ir.rootPage, BTreeTypeIndex)
	cells, err := btree.TraverseAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("traverse index %s B-tree: %w", ir.name, err)
	}

	return cells, nil
}

// GetRootPage returns the root page number
func (ir *IndexRawImpl) GetRootPage() int {
	return ir.rootPage
}

// GetName returns the index name
func (ir *IndexRawImpl) GetName() string {
	return ir.name
}

// SearchKeys searches for entries with the given key value using B-tree search
func (ir *IndexRawImpl) SearchKeys(ctx context.Context, key interface{}) ([]IndexEntry, error) {
	// Use B-tree search to find matching entries
	btree := NewBTree(ir.dbRaw, ir.rootPage, BTreeTypeIndex)
	cells, err := btree.Search(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("search index %s for key %v: %w", ir.name, key, err)
	}

	// Convert Cells to IndexEntries
	var entries []IndexEntry
	errorHandler := NewErrorHandler(ErrorStrategySkip, nil)

	for _, cell := range cells {
		entry, err := ir.cellToIndexEntry(cell)
		if handledErr := errorHandler.HandleProcessingError(err, "convert cell to index entry"); handledErr != nil {
			return nil, handledErr
		}
		if err == nil {
			entries = append(entries, *entry)
		}
	}

	return entries, nil
}

// GetIndexedColumns returns the columns covered by this index
func (ir *IndexRawImpl) GetIndexedColumns() []string {
	return ir.indexedColumns
}

// cellToIndexEntry converts a Cell to an index entry
func (ir *IndexRawImpl) cellToIndexEntry(cell Cell) (*IndexEntry, error) {
	// Index entries are stored differently than table rows
	// For simplicity, we'll assume the first values are the indexed columns
	// and use the cell's rowid

	if len(cell.Record.RecordBody.Values) == 0 {
		return nil, fmt.Errorf("empty index entry")
	}

	entry := &IndexEntry{
		Keys:  make([]Value, len(ir.indexedColumns)),
		Rowid: int64(cell.Rowid), // Convert uint64 to int64
	}

	// Extract key values from record body
	for i, rawValue := range cell.Record.RecordBody.Values {
		if i >= len(ir.indexedColumns) {
			break
		}

		// Convert raw value to SQLite value
		var serialType uint64 = 1 // Default to integer
		if i < len(cell.Record.RecordHeader.SerialTypes) {
			serialType = cell.Record.RecordHeader.SerialTypes[i]
		}

		data := ConvertToBytes(rawValue)
		entry.Keys[i] = NewSQLiteValue(serialType, data)
	}

	return entry, nil
}

// matchesKey checks if a value matches the search key
func (ir *IndexRawImpl) matchesKey(value Value, key interface{}) bool {
	valueData := value.Raw()
	if valueData == nil {
		return key == nil
	}

	valueStr := string(valueData)
	keyStr := fmt.Sprintf("%v", key)

	return valueStr == keyStr
}

// parseIndexColumns extracts the indexed columns from CREATE INDEX SQL
func parseIndexColumns(sql string) []string {
	// Parse the SQL to extract column names
	// This is a simplified parser - in practice, you might want to use a proper SQL parser

	// Remove extra whitespace and convert to uppercase for parsing
	cleanSQL := strings.ToUpper(strings.TrimSpace(sql))

	// Find the part between parentheses
	start := strings.Index(cleanSQL, "(")
	end := strings.LastIndex(cleanSQL, ")")

	if start == -1 || end == -1 || start >= end {
		return []string{}
	}

	columnsPart := cleanSQL[start+1 : end]
	columns := strings.Split(columnsPart, ",")

	// Clean up column names
	for i, col := range columns {
		columns[i] = strings.TrimSpace(col)
	}

	return columns
}

// parseIndexTableName extracts the table name from CREATE INDEX SQL
func parseIndexTableName(sql string) string {
	// Parse the SQL to extract table name
	// This is simplified - look for "ON table_name"

	cleanSQL := strings.ToUpper(strings.TrimSpace(sql))

	onIndex := strings.Index(cleanSQL, " ON ")
	if onIndex == -1 {
		return ""
	}

	afterOn := cleanSQL[onIndex+4:]
	parts := strings.Fields(afterOn)
	if len(parts) > 0 {
		tableName := parts[0]
		// Remove any parentheses
		if parenIndex := strings.Index(tableName, "("); parenIndex != -1 {
			tableName = tableName[:parenIndex]
		}
		return strings.ToLower(tableName) // Return lowercase for consistency
	}

	return ""
}
