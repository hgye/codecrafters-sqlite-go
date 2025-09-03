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

// ReadAllCells reads all cells from the index B-tree by proper traversal
func (ir *IndexRawImpl) ReadAllCells(ctx context.Context) ([]Cell, error) {
	// Start from root page and traverse the B-tree
	return ir.traverseIndexBTree(ctx, ir.rootPage)
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
	return ir.searchIndexBTree(ctx, ir.rootPage, key)
}

// GetIndexedColumns returns the columns covered by this index
func (ir *IndexRawImpl) GetIndexedColumns() []string {
	return ir.indexedColumns
}

// cellToIndexEntry converts a SQLite cell to an index entry
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

		data := ir.convertToBytes(rawValue)
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

// convertToBytes converts raw value to byte slice for storage
func (ir *IndexRawImpl) convertToBytes(rawValue interface{}) []byte {
	if rawValue == nil {
		return nil
	}

	if bytes, ok := rawValue.([]byte); ok {
		return bytes
	}

	return []byte(fmt.Sprintf("%v", rawValue))
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

// traverseIndexBTree traverses the index B-tree and returns all cells
func (ir *IndexRawImpl) traverseIndexBTree(ctx context.Context, pageNum int) ([]Cell, error) {
	pageData, err := ir.dbRaw.ReadPage(ctx, pageNum)
	if err != nil {
		return nil, fmt.Errorf("read index page %d: %w", pageNum, err)
	}

	pageHeader, err := ir.parsePageHeader(pageData)
	if err != nil {
		return nil, fmt.Errorf("parse page header for index page %d: %w", pageNum, err)
	}

	if pageHeader.IsLeafIndex() {
		// Leaf index page - read all cells from this page
		return ir.readIndexLeafCells(ctx, pageHeader, pageData)
	} else if pageHeader.IsInteriorIndex() {
		// Interior index page - traverse child pages
		return ir.traverseInteriorIndexPage(ctx, pageHeader, pageData)
	}

	return nil, fmt.Errorf("unexpected page type for index: 0x%02X", pageHeader.PageType)
}

// searchIndexBTree performs a B-tree search for the given key
func (ir *IndexRawImpl) searchIndexBTree(ctx context.Context, pageNum int, searchKey interface{}) ([]IndexEntry, error) {
	pageData, err := ir.dbRaw.ReadPage(ctx, pageNum)
	if err != nil {
		return nil, fmt.Errorf("read index page %d: %w", pageNum, err)
	}

	pageHeader, err := ir.parsePageHeader(pageData)
	if err != nil {
		return nil, fmt.Errorf("parse page header for index page %d: %w", pageNum, err)
	}


	if pageHeader.IsLeafIndex() {
		// Leaf page - search for matching entries
		return ir.searchLeafIndexPage(ctx, pageHeader, pageData, searchKey)
	} else if pageHeader.IsInteriorIndex() {
		// Interior page - find the right child to follow
		childPage := ir.findChildPageForKey(pageHeader, pageData, searchKey)
		return ir.searchIndexBTree(ctx, childPage, searchKey)
	}

	return nil, fmt.Errorf("unexpected page type for index search: 0x%02X", pageHeader.PageType)
}

// parsePageHeader parses a page header from page data
func (ir *IndexRawImpl) parsePageHeader(pageData []byte) (*PageHeader, error) {
	if len(pageData) < 8 {
		return nil, fmt.Errorf("page data too small for header: have %d bytes, need at least 8", len(pageData))
	}

	header := &PageHeader{
		PageType:         pageData[0],
		FirstFreeblock:   uint16(pageData[1])<<8 | uint16(pageData[2]),
		CellCount:        uint16(pageData[3])<<8 | uint16(pageData[4]),
		CellContentStart: uint16(pageData[5])<<8 | uint16(pageData[6]),
		FragmentedBytes:  pageData[7],
	}

	return header, nil
}

// readIndexLeafCells reads all cells from a leaf index page
func (ir *IndexRawImpl) readIndexLeafCells(ctx context.Context, pageHeader *PageHeader, pageData []byte) ([]Cell, error) {
	var cells []Cell
	cellPointerArrayOffset := 8 // Starts after page header

	for i := uint16(0); i < pageHeader.CellCount; i++ {
		// Read cell pointer
		pointerOffset := cellPointerArrayOffset + int(i*2)
		if pointerOffset+1 >= len(pageData) {
			return nil, fmt.Errorf("cell pointer array overflow at offset %d", pointerOffset)
		}
		cellOffset := int(uint16(pageData[pointerOffset])<<8 | uint16(pageData[pointerOffset+1]))

		// Read the cell
		cell, err := ir.readIndexLeafCell(pageData, cellOffset)
		if err != nil {
			return nil, fmt.Errorf("read leaf cell %d: %w", i, err)
		}
		cells = append(cells, *cell)
	}

	return cells, nil
}

// readIndexLeafCell reads a single cell from a leaf index page
func (ir *IndexRawImpl) readIndexLeafCell(pageData []byte, offset int) (*Cell, error) {
	if offset >= len(pageData) {
		return nil, fmt.Errorf("cell offset %d exceeds page size %d", offset, len(pageData))
	}

	// Leaf index cell format: varint payload_size, payload
	payloadSize, bytesRead := readVarint(pageData, offset)
	offset += bytesRead

	if offset+int(payloadSize) > len(pageData) {
		return nil, fmt.Errorf("payload extends beyond page boundary")
	}
	payload := pageData[offset : offset+int(payloadSize)]

	// Parse record from payload
	record, err := ir.parseRecord(payload)
	if err != nil {
		return nil, err
	}

	// For index cells, the last value in the record is the rowid
	var rowid uint64
	if len(record.RecordBody.Values) > 0 {
		// Extract rowid from the last value
		lastValue := record.RecordBody.Values[len(record.RecordBody.Values)-1]
		if bytes, ok := lastValue.([]byte); ok && len(bytes) > 0 {
			// Convert bytes to integer rowid
			for _, b := range bytes {
				rowid = (rowid << 8) | uint64(b)
			}
		}
	}

	return &Cell{
		PayloadSize: payloadSize,
		Rowid:       rowid,
		Record:      *record,
	}, nil
}

// traverseInteriorIndexPage traverses an interior index page
func (ir *IndexRawImpl) traverseInteriorIndexPage(ctx context.Context, pageHeader *PageHeader, pageData []byte) ([]Cell, error) {
	if len(pageData) < 12 { // Interior pages have rightmost pointer
		return nil, fmt.Errorf("interior index page too small: have %d bytes, need at least 12", len(pageData))
	}

	// Read rightmost child pointer (bytes 8-11)
	rightmostChild := uint32(pageData[8])<<24 | uint32(pageData[9])<<16 | uint32(pageData[10])<<8 | uint32(pageData[11])

	var allCells []Cell
	cellPointerArrayOffset := 12 // Starts after page header and rightmost pointer

	// Process all child pages referenced by interior cells
	for i := uint16(0); i < pageHeader.CellCount; i++ {
		// Read cell pointer
		pointerOffset := cellPointerArrayOffset + int(i*2)
		if pointerOffset+1 >= len(pageData) {
			return nil, fmt.Errorf("cell pointer array overflow at offset %d", pointerOffset)
		}
		cellOffset := int(uint16(pageData[pointerOffset])<<8 | uint16(pageData[pointerOffset+1]))

		// Read child page number from interior cell
		childPageNum, err := ir.readInteriorIndexCellChildPage(pageData, cellOffset)
		if err != nil {
			return nil, fmt.Errorf("read interior cell child page: %w", err)
		}

		// Recursively traverse child page
		childCells, err := ir.traverseIndexBTree(ctx, int(childPageNum))
		if err != nil {
			return nil, fmt.Errorf("traverse child page %d: %w", childPageNum, err)
		}
		allCells = append(allCells, childCells...)
	}

	// Process rightmost child page
	rightmostCells, err := ir.traverseIndexBTree(ctx, int(rightmostChild))
	if err != nil {
		return nil, fmt.Errorf("traverse rightmost child page %d: %w", rightmostChild, err)
	}
	allCells = append(allCells, rightmostCells...)

	return allCells, nil
}

// readInteriorIndexCellChildPage reads the child page number from an interior index cell
func (ir *IndexRawImpl) readInteriorIndexCellChildPage(pageData []byte, offset int) (uint32, error) {
	if offset+4 > len(pageData) {
		return 0, fmt.Errorf("interior cell offset %d exceeds page size", offset)
	}

	// Interior index cell format: 4-byte child page number, varint payload_size, payload (key)
	childPageNum := uint32(pageData[offset])<<24 | uint32(pageData[offset+1])<<16 | uint32(pageData[offset+2])<<8 | uint32(pageData[offset+3])
	return childPageNum, nil
}

// findChildPageForKey finds the appropriate child page for a search key
func (ir *IndexRawImpl) findChildPageForKey(pageHeader *PageHeader, pageData []byte, searchKey interface{}) int {
	if len(pageData) < 12 {
		return 0 // Invalid page
	}

	// Read rightmost child pointer
	rightmostChild := uint32(pageData[8])<<24 | uint32(pageData[9])<<16 | uint32(pageData[10])<<8 | uint32(pageData[11])
	cellPointerArrayOffset := 12


	// Iterate through cells to find the right child
	for i := uint16(0); i < pageHeader.CellCount; i++ {
		pointerOffset := cellPointerArrayOffset + int(i*2)
		if pointerOffset+1 >= len(pageData) {
			break
		}
		cellOffset := int(uint16(pageData[pointerOffset])<<8 | uint16(pageData[pointerOffset+1]))

		// Read the cell to get the key
		childPageNum, key, err := ir.readInteriorIndexCell(pageData, cellOffset)
		if err != nil {
			continue
		}

		cmp := ir.compareKeys(searchKey, key)

		// Compare search key with cell key
		if cmp <= 0 {
			// Search key is less than or equal to this cell's key
			return int(childPageNum)
		}
	}

	// If we get here, search key is greater than all cell keys
	return int(rightmostChild)
}

// readInteriorIndexCell reads an interior index cell and returns child page and key
func (ir *IndexRawImpl) readInteriorIndexCell(pageData []byte, offset int) (uint32, interface{}, error) {
	if offset+4 > len(pageData) {
		return 0, nil, fmt.Errorf("interior cell offset %d exceeds page size", offset)
	}

	// Read child page number
	childPageNum := uint32(pageData[offset])<<24 | uint32(pageData[offset+1])<<16 | uint32(pageData[offset+2])<<8 | uint32(pageData[offset+3])
	offset += 4

	// Read payload size
	payloadSize, bytesRead := readVarint(pageData, offset)
	offset += bytesRead

	if offset+int(payloadSize) > len(pageData) {
		return 0, nil, fmt.Errorf("payload extends beyond page boundary")
	}
	payload := pageData[offset : offset+int(payloadSize)]

	// Parse the key from payload
	record, err := ir.parseRecord(payload)
	if err != nil {
		return childPageNum, nil, err
	}

	// Extract the first key value for comparison
	var key interface{}
	if len(record.RecordBody.Values) > 0 {
		// Convert bytes to string for proper comparison
		if bytes, ok := record.RecordBody.Values[0].([]byte); ok {
			key = string(bytes)
		} else {
			key = record.RecordBody.Values[0]
		}
	}

	return childPageNum, key, nil
}

// searchLeafIndexPage searches for matching entries in a leaf index page
func (ir *IndexRawImpl) searchLeafIndexPage(ctx context.Context, pageHeader *PageHeader, pageData []byte, searchKey interface{}) ([]IndexEntry, error) {
	var entries []IndexEntry
	cellPointerArrayOffset := 8


	for i := uint16(0); i < pageHeader.CellCount; i++ {
		// Read cell pointer
		pointerOffset := cellPointerArrayOffset + int(i*2)
		if pointerOffset+1 >= len(pageData) {
			break
		}
		cellOffset := int(uint16(pageData[pointerOffset])<<8 | uint16(pageData[pointerOffset+1]))

		// Read the cell
		cell, err := ir.readIndexLeafCell(pageData, cellOffset)
		if err != nil {
			continue
		}

		// Convert cell to index entry
		entry, err := ir.cellToIndexEntry(*cell)
		if err != nil {
			continue
		}

		// Check if entry matches search key
		if len(entry.Keys) > 0 && ir.matchesKey(entry.Keys[0], searchKey) {
			entries = append(entries, *entry)
		}
	}

	return entries, nil
}

// compareKeys compares two keys for B-tree navigation
func (ir *IndexRawImpl) compareKeys(key1, key2 interface{}) int {
	// Simple string comparison for now
	str1 := fmt.Sprintf("%v", key1)
	str2 := fmt.Sprintf("%v", key2)

	if str1 < str2 {
		return -1
	} else if str1 > str2 {
		return 1
	}
	return 0
}

// parseRecord parses a record from payload data
func (ir *IndexRawImpl) parseRecord(payload []byte) (*Record, error) {
	header, offset := readRecordHeader(payload, 0)
	body, _, err := readRecordBody(payload, offset, header)
	if err != nil {
		return nil, err
	}

	return &Record{
		RecordHeader: header,
		RecordBody:   body,
	}, nil
}
