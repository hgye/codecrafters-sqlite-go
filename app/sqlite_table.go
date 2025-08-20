package main

import (
	"fmt"
)

// Cell represents a B-tree cell (varies by page type)
type Cell struct {
	// For Table B-Tree Leaf Cell (header 0x0d):
	PayloadSize uint64 // varint: total bytes of payload including overflow
	Rowid       uint64 // varint: integer key (rowid)
	Record      Record // parsed record from payload
	// OverflowPage uint32 // 4-byte page number for overflow (if needed)
}

// Record represents a record within a cell
type Record struct {
	RecordHeader
	RecordBody
}

// RecordHeader represents the header of a record in the payload
type RecordHeader struct {
	HeaderSize  uint64  // varint: total bytes in header including this varint
	SerialTypes []uint8 // serial types: one per column, determines datatype
}

// RecordBody represents the body/data portion of a record
type RecordBody struct {
	Values []interface{} // actual column values based on serial types

	// Union-style access for specific table types
	Schema *SchemaRecord // When this is a sqlite_schema/sqlite_master record
}

// SchemaRecord represents a record from the sqlite_schema/sqlite_master table
type SchemaRecord struct {
	Type     string // "table", "index", "view", "trigger"
	Name     string // object name
	TblName  string // table name (for indexes, this is the table they belong to)
	RootPage uint8  // root page number in the database file (single byte)
	SQL      string // CREATE statement for this object
}

// SerialType constants for SQLite record format
const (
	SerialTypeNull    = 0
	SerialTypeInt8    = 1
	SerialTypeInt16   = 2
	SerialTypeInt24   = 3
	SerialTypeInt32   = 4
	SerialTypeInt48   = 5
	SerialTypeInt64   = 6
	SerialTypeFloat64 = 7
	SerialTypeZero    = 8 // schema format 4+
	SerialTypeOne     = 9 // schema format 4+
	// SerialType >= 12 and even: BLOB with (N-12)/2 bytes
	// SerialType >= 13 and odd: TEXT with (N-13)/2 bytes
)

// Table represents a SQLite table with its metadata and data access methods
type Table struct {
	Name      string
	SchemaSQL string
	RootPage  uint8     // root page number in the database file (single byte)
	db        *SQLiteDB // reference to parent database
}

// NewTable creates a new Table instance from schema information
func NewTable(name, schemaSQL string, rootPage uint8, db *SQLiteDB) *Table {
	return &Table{
		Name:      name,
		SchemaSQL: schemaSQL,
		RootPage:  rootPage,
		db:        db,
	}
}

// NewTableFromSchemaCell creates a Table from a schema table cell
func NewTableFromSchemaCell(cell *Cell, db *SQLiteDB) *Table {
	schema := cell.Record.RecordBody.ParseAsSchema()
	if schema == nil || schema.Type != "table" {
		return nil // Not a table record
	}

	return &Table{
		Name:      schema.Name,
		SchemaSQL: schema.SQL,
		RootPage:  schema.RootPage,
		db:        db,
	}
}

// readVarint reads a variable-length integer from the data
func readVarint(data []byte, offset int) (value uint64, bytesRead int) {
	var result uint64
	for i := 0; i < 9 && offset+i < len(data); i++ {
		b := data[offset+i]
		if i == 8 {
			// 9th byte uses all 8 bits
			result = (result << 8) | uint64(b)
			return result, i + 1
		}
		// First 8 bytes use lower 7 bits
		result = (result << 7) | uint64(b&0x7F)
		if (b & 0x80) == 0 {
			// High bit clear means this is the last byte
			return result, i + 1
		}
	}
	return result, 0 // Invalid varint
}

// getSerialTypeSize returns the size in bytes for a given serial type
func getSerialTypeSize(serialType uint8) int {
	switch serialType {
	case 0, 8, 9:
		return 0
	case 1:
		return 1
	case 2:
		return 2
	case 3:
		return 3
	case 4:
		return 4
	case 5:
		return 6
	case 6, 7:
		return 8
	default:
		if serialType >= 12 && serialType%2 == 0 {
			// BLOB: (N-12)/2 bytes
			return int((serialType - 12) / 2)
		} else if serialType >= 13 && serialType%2 == 1 {
			// TEXT: (N-13)/2 bytes
			return int((serialType - 13) / 2)
		}
		return 0
	}
}

// readCell reads a cell from the specified cell pointer using the database file
func (t *Table) readCell(cellPointer CellPointer) (*Cell, error) {
	return t.db.readCell(cellPointer)
}

// readRecordHeader reads and parses a record header from payload data
func readRecordHeader(data []byte, offset int) (RecordHeader, int) {
	var header RecordHeader
	var bytesRead int
	header.HeaderSize, bytesRead = readVarint(data, offset)
	offset += bytesRead

	if header.HeaderSize == 0 {
		return header, offset // No header
	}

	// Calculate how many serial types we need to read
	headerEnd := int(header.HeaderSize)
	for offset < headerEnd {
		var serialType uint64
		serialType, bytesRead = readVarint(data, offset)
		header.SerialTypes = append(header.SerialTypes, uint8(serialType))
		offset += bytesRead
	}

	return header, offset
}

// readRecordBody reads and parses a record body from payload data
func readRecordBody(data []byte, offset int, header RecordHeader) (RecordBody, int, error) {
	var body RecordBody
	body.Values = make([]interface{}, len(header.SerialTypes))

	for i, serialType := range header.SerialTypes {
		size := getSerialTypeSize(serialType)
		if size == 0 {
			body.Values[i] = nil // NULL value
			continue
		}
		if offset+size > len(data) {
			return body, offset, fmt.Errorf("not enough data for record body: need %d bytes, have %d", offset+size, len(data))
		}
		value := data[offset : offset+size]
		body.Values[i] = value // Store raw bytes for now
		offset += size
	}
	return body, offset, nil
}

// ParseAsSchema parses the record body as a schema table record
func (rb *RecordBody) ParseAsSchema() *SchemaRecord {
	if len(rb.Values) < 5 {
		return nil // Invalid schema record
	}

	schema := &SchemaRecord{}

	// Parse each field from the Values slice
	if rb.Values[0] != nil {
		schema.Type = string(rb.Values[0].([]byte))
	}
	if rb.Values[1] != nil {
		schema.Name = string(rb.Values[1].([]byte))
	}
	if rb.Values[2] != nil {
		schema.TblName = string(rb.Values[2].([]byte))
	}
	if rb.Values[3] != nil {
		// Parse rootpage as integer from bytes
		rootPageBytes := rb.Values[3].([]byte)
		if len(rootPageBytes) > 0 {
			schema.RootPage = rootPageBytes[0] // Single byte for root page
			// log.Printf("DEBUG: RootPage parsed as: %d", schema.RootPage)
		}
	}
	if rb.Values[4] != nil {
		schema.SQL = string(rb.Values[4].([]byte))
		// log.Printf("DEBUG: SQL parsed as: %s", schema.SQL)
	}

	// Set the union field
	rb.Schema = schema
	return schema
}

// IsSchemaRecord checks if this record appears to be from sqlite_schema/sqlite_master
func (rb *RecordBody) IsSchemaRecord() bool {
	if len(rb.Values) != 5 {
		return false
	}

	// Check if first field looks like a schema type
	if rb.Values[0] != nil {
		typeStr := string(rb.Values[0].([]byte))
		return typeStr == "table" || typeStr == "index" || typeStr == "view" || typeStr == "trigger"
	}

	return false
}

// ReadColumn reads and prints data from a specific column in the table
func (t *Table) ReadColumn(columnIndex int) error {
	cells, err := t.GetAllRows()
	if err != nil {
		return err
	}

	for _, cell := range cells {
		if len(cell.Record.RecordBody.Values) > columnIndex {
			value := cell.Record.RecordBody.Values[columnIndex]
			if value != nil {
				t.printValue(value)
			}
		}
	}

	return nil
}

// printValue prints a value with proper type formatting
func (t *Table) printValue(value interface{}) {
	switch v := value.(type) {
	case []byte:
		fmt.Println(string(v))
	case string:
		fmt.Println(v)
	case int64:
		fmt.Println(v)
	case float64:
		fmt.Println(v)
	default:
		fmt.Printf("%v\n", v)
	}
}

// readTableCell reads a table B-tree leaf cell (type 0x0d) from the current file position
func (t *Table) readTableCell() (*Cell, error) {
	var cell Cell

	// Remember the starting position
	cellStart, err := t.db.Seek(0, 1)
	if err != nil {
		return nil, err
	}

	// Read payload size and rowid varints from file
	payloadData := make([]byte, 64) // Read enough bytes to parse varints
	if _, err = t.db.Read(payloadData); err != nil {
		return nil, err
	}

	var bytesRead int
	cell.PayloadSize, bytesRead = readVarint(payloadData, 0)
	var rowidBytesRead int
	cell.Rowid, rowidBytesRead = readVarint(payloadData, bytesRead)
	totalVarintBytes := bytesRead + rowidBytesRead

	// Calculate payload start directly from cell start
	payloadStart := cellStart + int64(totalVarintBytes)
	if _, err = t.db.Seek(payloadStart, 0); err != nil {
		return nil, err
	}

	// Read the actual payload data
	payloadSize := int(cell.PayloadSize)
	payload := make([]byte, payloadSize)
	if _, err = t.db.Read(payload); err != nil {
		return nil, err
	}

	// Parse record from payload
	var record Record
	var headerOffset int
	record.RecordHeader, headerOffset = readRecordHeader(payload, 0)
	record.RecordBody, _, err = readRecordBody(payload, headerOffset, record.RecordHeader)
	if err != nil {
		return nil, err
	}
	cell.Record = record

	return &cell, nil
}

// GetAllRows returns all rows from this table using modern readers
func (t *Table) GetAllRows() ([]*Cell, error) {
	pageReader := NewPageReader(t.db, t.db.header.PageSize)
	varintReader := NewVarintReader(t.db)

	return t.readAllCellsWithReaders(pageReader, varintReader)
}

// readAllCellsWithReaders performs the actual cell reading using provided readers
func (t *Table) readAllCellsWithReaders(pageReader *PageReader, varintReader *VarintReader) ([]*Cell, error) {
	// Read page header
	pageHeader, err := pageReader.ReadPageHeader(uint32(t.RootPage))
	if err != nil {
		return nil, NewDatabaseError("read_table_page_header", err, map[string]interface{}{
			"table_name": t.Name,
			"root_page":  t.RootPage,
		})
	}

	// Validate page type
	if pageHeader.PageType != 0x0d {
		return nil, NewDatabaseError("validate_table_page_type", ErrInvalidPageType, map[string]interface{}{
			"table_name":    t.Name,
			"expected_type": 0x0d,
			"actual_type":   pageHeader.PageType,
		})
	}

	// Read cell pointers
	cellPointers, err := pageReader.ReadCellPointers(uint32(t.RootPage), pageHeader.CellCount)
	if err != nil {
		return nil, NewDatabaseError("read_table_cell_pointers", err, map[string]interface{}{
			"table_name": t.Name,
			"cell_count": pageHeader.CellCount,
		})
	}

	// Read all cells using the original approach that works
	cells := make([]*Cell, 0, len(cellPointers))
	for i, cellPointer := range cellPointers {
		// Calculate absolute offset
		pageOffset := t.calculatePageOffset(uint32(t.RootPage))
		cellAbsoluteOffset := pageOffset + int64(cellPointer.Offset())

		// Seek to the absolute cell position
		_, err := t.db.Seek(cellAbsoluteOffset, 0)
		if err != nil {
			return nil, NewDatabaseError("seek_to_cell", err, map[string]interface{}{
				"table_name":  t.Name,
				"cell_index":  i,
				"cell_offset": cellAbsoluteOffset,
			})
		}

		// Read table B-tree leaf cell using the original working method
		cell, err := t.readTableCell()
		if err != nil {
			return nil, NewDatabaseError("read_table_cell", err, map[string]interface{}{
				"table_name":   t.Name,
				"cell_index":   i,
				"cell_pointer": cellPointer,
			})
		}

		cells = append(cells, cell)
	}

	return cells, nil
}

// calculatePageOffset calculates the byte offset for a page number
func (t *Table) calculatePageOffset(pageNum uint32) int64 {
	if pageNum == 1 {
		return 100 // First page starts after 100-byte header
	}
	return int64((pageNum - 1) * uint32(t.db.header.PageSize))
}

// GetRowCount returns the number of rows in this table
func (t *Table) GetRowCount() (int, error) {
	rows, err := t.GetAllRows()
	if err != nil {
		return 0, err
	}
	return len(rows), nil
}

// String returns a string representation of the table
func (t *Table) String() string {
	return t.Name
}
