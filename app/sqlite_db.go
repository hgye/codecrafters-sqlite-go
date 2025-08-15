package main

import (
	"encoding/binary"
	"fmt"
	"os"
)

// SQLiteDB represents a SQLite database file
type SQLiteDB struct {
	file   *os.File
	header *DatabaseHeader
	schema []*Cell // Schema table (sqlite_master) content
}

// DatabaseHeader represents the 100-byte SQLite database file header
type DatabaseHeader struct {
	MagicNumber     [16]byte
	PageSize        uint16
	FileFormatWrite uint8
	FileFormatRead  uint8
	ReservedBytes   uint8
	MaxPayload      uint8
	MinPayload      uint8
	LeafPayload     uint8
	FileChangeCount uint32
	DatabaseSize    uint32
	FirstFreePage   uint32
	FreePageCount   uint32
	SchemaCookie    uint32
	SchemaFormat    uint32
	DefaultCache    uint32
	LargestBTree    uint32
	TextEncoding    uint32
	UserVersion     uint32
	IncrVacuum      uint32
	AppID           uint32
	Reserved        [20]byte
	VersionValid    uint32
	SQLiteVersion   uint32
}

// PageHeader represents a B-tree page header
type PageHeader struct {
	PageType         uint8
	FirstFreeblock   uint16
	CellCount        uint16
	CellContentStart uint16
	FragmentedBytes  uint8
	// RightmostPointer uint32 // Only for interior pages
}

// CellPointer represents a pointer to a cell within a page
type CellPointer uint16

// NewSQLiteDB opens a SQLite database file and returns a SQLiteDB instance
func NewSQLiteDB(filePath string) (*SQLiteDB, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database file: %w", err)
	}

	db := &SQLiteDB{
		file: file,
	}

	// Parse the database header
	if err := db.parseHeader(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to parse database header: %w", err)
	}

	// Load the schema table (sqlite_master)
	if err := db.loadSchema(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to load schema: %w", err)
	}

	return db, nil
}

// Close closes the database file
func (db *SQLiteDB) Close() error {
	if db.file != nil {
		return db.file.Close()
	}
	return nil
}

// parseHeader reads and parses the 100-byte database header
func (db *SQLiteDB) parseHeader() error {
	if _, err := db.file.Seek(0, 0); err != nil {
		return err
	}

	var header DatabaseHeader
	if err := binary.Read(db.file, binary.BigEndian, &header); err != nil {
		return err
	}

	db.header = &header
	return nil
}

// GetHeader returns the database header
func (db *SQLiteDB) GetHeader() *DatabaseHeader {
	return db.header
}

// GetPageSize returns the page size from the header
func (db *SQLiteDB) GetPageSize() uint16 {
	return db.header.PageSize
}

// readPageHeader reads a B-tree page header from the current file position
func (db *SQLiteDB) readPageHeader() (*PageHeader, error) {
	var pageHeader PageHeader
	if err := binary.Read(db.file, binary.BigEndian, &pageHeader); err != nil {
		return nil, err
	}
	return &pageHeader, nil
}

// readCellPointerArray reads the cell pointer array from a page
func (db *SQLiteDB) readCellPointerArray(cellCount uint16) ([]CellPointer, error) {
	cellPointers := make([]uint16, cellCount)
	if err := binary.Read(db.file, binary.BigEndian, &cellPointers); err != nil {
		return nil, err
	}

	// Convert []uint16 to []CellPointer
	result := make([]CellPointer, cellCount)
	for i, pointer := range cellPointers {
		result[i] = CellPointer(pointer)
	}
	return result, nil
}

// readCell reads a cell from the specified cell pointer
func (db *SQLiteDB) readCell(cellPointer CellPointer) (*Cell, error) {
	offset := cellPointer.Offset()
	if _, err := db.file.Seek(int64(offset), 0); err != nil {
		return nil, err
	}

	var cell Cell

	// Read payload size and rowid varints from file
	payloadData := make([]byte, 64) // Read enough bytes to parse varints
	if _, err := db.file.Read(payloadData); err != nil {
		return nil, err
	}

	var bytesRead int
	cell.PayloadSize, bytesRead = readVarint(payloadData, 0)
	var rowidBytesRead int
	cell.Rowid, rowidBytesRead = readVarint(payloadData, bytesRead)
	totalVarintBytes := bytesRead + rowidBytesRead

	// Read the actual payload data
	payloadSize := int(cell.PayloadSize)
	payload := make([]byte, payloadSize)
	if _, err := db.file.Seek(int64(cellPointer.Offset())+int64(totalVarintBytes), 0); err != nil {
		return nil, err
	}
	if _, err := db.file.Read(payload); err != nil {
		return nil, err
	}

	// Parse record from payload (always one record per cell in table b-tree leaf)
	var record Record
	var headerOffset int
	record.RecordHeader, headerOffset = readRecordHeader(payload, 0)
	record.RecordBody, _ = readRecordBody(payload, headerOffset, record.RecordHeader)
	cell.Record = record

	return &cell, nil
}

// loadSchema loads the schema table (sqlite_master) content
func (db *SQLiteDB) loadSchema() error {
	// Seek to the first page header (after the 100-byte database header)
	if _, err := db.file.Seek(100, 0); err != nil {
		return err
	}

	// Read the first page header (sqlite_master table)
	pageHeader, err := db.readPageHeader()
	if err != nil {
		return err
	}

	// Read cell pointer array
	cellPointers, err := db.readCellPointerArray(pageHeader.CellCount)
	if err != nil {
		return err
	}

	// Read all cells from the schema table
	db.schema = make([]*Cell, 0, len(cellPointers))
	for _, pointer := range cellPointers {
		cell, err := db.readCell(pointer)
		if err != nil {
			return err
		}
		db.schema = append(db.schema, cell)
	}

	return nil
}

// GetSchema returns the schema table content
func (db *SQLiteDB) GetSchema() []*Cell {
	return db.schema
}

// GetTableNames returns a list of table names from the schema
func (db *SQLiteDB) GetTableNames() []string {
	var tables []string
	tables = append(tables, "sqlite_master") // First table is always the schema table

	for _, cell := range db.schema {
		schema := cell.Record.RecordBody.ParseAsSchema()
		if schema != nil && schema.Type == "table" && schema.Name != "sqlite_master" {
			tables = append(tables, schema.Name)
		}
	}

	return tables
}

// GetTables returns a list of Table objects from the schema
func (db *SQLiteDB) GetTables() []*Table {
	var tables []*Table

	for _, cell := range db.schema {
		schema := cell.Record.RecordBody.ParseAsSchema()
		if schema != nil && schema.Type == "table" {
			table := NewTableFromSchemaCell(cell, db)
			if table != nil {
				tables = append(tables, table)
			}
		}
	}

	return tables
}

// GetTableCount returns the number of tables in the database
func (db *SQLiteDB) GetTableCount() int {
	if db.header == nil {
		return 0
	}
	// Note: This returns the cell count from the first page, which includes all schema objects
	// For actual table count, you'd need to filter the schema
	return len(db.schema)
}

// Offset returns the uint16 offset value
func (cp CellPointer) Offset() uint16 {
	return uint16(cp)
}

// IsValid checks if the cell pointer is valid (non-zero)
func (cp CellPointer) IsValid() bool {
	return cp > 0
}

// GetSchemaObjects returns all schema objects (tables, indexes, views, triggers)
func (db *SQLiteDB) GetSchemaObjects() []*SchemaRecord {
	var objects []*SchemaRecord

	for _, cell := range db.schema {
		schema := cell.Record.RecordBody.ParseAsSchema()
		if schema != nil {
			objects = append(objects, schema)
		}
	}

	return objects
}
