package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"strings"

	"github.com/xwb1989/sqlparser"
)

// SQLiteDB represents a SQLite database file
type SQLiteDB struct {
	file          *os.File
	header        *DatabaseHeader
	schemaTable   []*Cell         // Schema table (sqlite_master) cell content
	schemaRecords []*SchemaRecord // Cached parsed schema records
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
		return nil, NewDatabaseError("open_database_file", err, map[string]interface{}{
			"file_path": filePath,
		})
	}

	db := &SQLiteDB{
		file: file,
	}

	// Parse the database header
	if err := db.parseHeader(); err != nil {
		file.Close()
		return nil, NewDatabaseError("parse_database_header", err, nil)
	}

	// Load the schema table (sqlite_master)
	if err := db.loadSchema(); err != nil {
		file.Close()
		return nil, NewDatabaseError("load_schema", err, nil)
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

// Read implements io.Reader
func (db *SQLiteDB) Read(p []byte) (n int, err error) {
	return db.file.Read(p)
}

// ReadAt implements io.ReaderAt
func (db *SQLiteDB) ReadAt(p []byte, off int64) (n int, err error) {
	return db.file.ReadAt(p, off)
}

// Seek implements io.Seeker
func (db *SQLiteDB) Seek(offset int64, whence int) (int64, error) {
	return db.file.Seek(offset, whence)
}

// GetFile returns the underlying file for backward compatibility
func (db *SQLiteDB) GetFile() *os.File {
	return db.file
}

// parseHeader reads and parses the 100-byte database header
func (db *SQLiteDB) parseHeader() error {
	if _, err := db.Seek(0, 0); err != nil {
		return err
	}

	var header DatabaseHeader
	if err := binary.Read(db, binary.BigEndian, &header); err != nil {
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
	if err := binary.Read(db, binary.BigEndian, &pageHeader); err != nil {
		return nil, err
	}
	return &pageHeader, nil
}

// readCellPointerArray reads the cell pointer array from a page at the specified page offset
func (db *SQLiteDB) readCellPointerArray(cellCount uint16, pageOffset int64) ([]CellPointer, error) {
	// Cell pointer array starts immediately after the page header
	// The caller (readPageHeader) should have left the file position at the start of the cell pointer array
	// We include pageOffset parameter for explicit documentation of where we're reading from

	cellPointers := make([]uint16, cellCount)
	if err := binary.Read(db, binary.BigEndian, &cellPointers); err != nil {
		return nil, NewDatabaseError("read_cell_pointer_array", err, map[string]interface{}{
			"page_offset": pageOffset,
			"cell_count":  cellCount,
		})
	}

	// Convert []uint16 to []CellPointer and validate
	result := make([]CellPointer, cellCount)
	for i, pointer := range cellPointers {
		result[i] = CellPointer(pointer)
		// Validate that cell pointer is within reasonable range (not zero, within page bounds)
		if pointer == 0 || pointer > uint16(db.header.PageSize) {
			return nil, NewDatabaseError("validate_cell_pointer", ErrInvalidCellPointer, map[string]interface{}{
				"pointer_index": i,
				"pointer_value": pointer,
				"page_size":     db.header.PageSize,
			})
		}
	}
	return result, nil
}

// readCell reads a cell from the specified cell pointer
func (db *SQLiteDB) readCell(cellPointer CellPointer) (*Cell, error) {
	offset := cellPointer.Offset()
	if _, err := db.Seek(int64(offset), 0); err != nil {
		return nil, NewDatabaseError("seek_cell_position", err, map[string]interface{}{
			"offset": offset,
		})
	}

	var cell Cell

	// Read payload size and rowid varints from file
	payloadData := make([]byte, 64) // Read enough bytes to parse varints
	if _, err := db.Read(payloadData); err != nil {
		return nil, NewDatabaseError("read_cell_varints", err, nil)
	}

	var bytesRead int
	cell.PayloadSize, bytesRead = readVarint(payloadData, 0)
	var rowidBytesRead int
	cell.Rowid, rowidBytesRead = readVarint(payloadData, bytesRead)
	totalVarintBytes := bytesRead + rowidBytesRead

	// Read the actual payload data
	payloadSize := int(cell.PayloadSize)
	payload := make([]byte, payloadSize)
	if _, err := db.Seek(int64(cellPointer.Offset())+int64(totalVarintBytes), 0); err != nil {
		return nil, NewDatabaseError("seek_payload_position", err, map[string]interface{}{
			"payload_offset": int64(cellPointer.Offset()) + int64(totalVarintBytes),
		})
	}
	if _, err := db.Read(payload); err != nil {
		return nil, NewDatabaseError("read_payload_data", err, map[string]interface{}{
			"payload_size": payloadSize,
		})
	}

	// Parse record from payload (always one record per cell in table b-tree leaf)
	var record Record
	var headerOffset int
	record.RecordHeader, headerOffset = readRecordHeader(payload, 0)
	var err error
	record.RecordBody, _, err = readRecordBody(payload, headerOffset, record.RecordHeader)
	if err != nil {
		return nil, NewDatabaseError("parse_record_body", err, nil)
	}
	cell.Record = record

	return &cell, nil
}

// loadSchema loads the schema table (sqlite_master) content
func (db *SQLiteDB) loadSchema() error {
	// Seek to the first page header (after the 100-byte database header)
	pageOffset := int64(100)
	if _, err := db.Seek(pageOffset, 0); err != nil {
		return NewDatabaseError("seek_schema_page", err, map[string]interface{}{
			"page_offset": pageOffset,
		})
	}

	// Read the first page header (sqlite_master table)
	pageHeader, err := db.readPageHeader()
	if err != nil {
		return NewDatabaseError("read_schema_page_header", err, nil)
	}

	// Read cell pointer array (file position is now at start of cell pointer array)
	cellPointers, err := db.readCellPointerArray(pageHeader.CellCount, pageOffset)
	if err != nil {
		return err // Already wrapped in NewDatabaseError
	}

	// Read all cells from the schema table
	db.schemaTable = make([]*Cell, 0, len(cellPointers))
	db.schemaRecords = make([]*SchemaRecord, 0, len(cellPointers))

	for i, pointer := range cellPointers {
		cell, err := db.readCell(pointer)
		if err != nil {
			return NewDatabaseError("read_schema_cell", err, map[string]interface{}{
				"cell_index":   i,
				"cell_pointer": pointer,
			})
		}
		db.schemaTable = append(db.schemaTable, cell)

		// Parse and cache the schema record
		schema := cell.Record.RecordBody.ParseAsSchema()
		db.schemaRecords = append(db.schemaRecords, schema)
	}

	return nil
}

// GetSchema returns the schema table content
func (db *SQLiteDB) GetSchema() []*Cell {
	return db.schemaTable
}

// GetTableNames returns a list of table names from the schema
func (db *SQLiteDB) GetTableNames() []string {
	var tables []string
	tables = append(tables, "sqlite_master") // First table is always the schema table

	for _, schema := range db.schemaRecords {
		if schema != nil && schema.Type == "table" && schema.Name != "sqlite_master" {
			tables = append(tables, schema.Name)
		}
	}

	return tables
}

// GetTables returns a list of Table objects from the schema
func (db *SQLiteDB) GetTables() []*Table {
	var tables []*Table

	for i, schema := range db.schemaRecords {
		if schema != nil && schema.Type == "table" {
			table := NewTableFromSchemaCell(db.schemaTable[i], db)
			if table != nil {
				tables = append(tables, table)
			}
		}
	}

	return tables
}

// GetTable returns a specific table by name, or nil if not found
func (db *SQLiteDB) GetTable(tableName string) *Table {
	for i, schema := range db.schemaRecords {
		if schema != nil && schema.Type == "table" && schema.Name == tableName {
			return NewTableFromSchemaCell(db.schemaTable[i], db)
		}
	}
	return nil
}

// GetTableCount returns the number of tables in the database
func (db *SQLiteDB) GetTableCount() int {
	if db.header == nil {
		return 0
	}
	// Note: This returns the cell count from the first page, which includes all schema objects
	// For actual table count, you'd need to filter the schema
	return len(db.schemaTable)
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

	for _, schema := range db.schemaRecords {
		if schema != nil {
			objects = append(objects, schema)
		}
	}

	return objects
}

// GetTableSchema returns the schema for a table
func (db *SQLiteDB) GetTableSchema(tableName string) ([]*Column, error) {
	table := db.GetTable(tableName)
	if table == nil {
		return nil, NewDatabaseError("get_table_schema", ErrTableNotFound, map[string]interface{}{
			"table_name": tableName,
		})
	}

	return db.parseTableSchema(table.SchemaSQL)
}

// GetTableRows returns all rows from a table
func (db *SQLiteDB) GetTableRows(tableName string) ([]*Row, error) {
	table := db.GetTable(tableName)
	if table == nil {
		return nil, NewDatabaseError("get_table_rows", ErrTableNotFound, map[string]interface{}{
			"table_name": tableName,
		})
	}

	// Delegate to table for physical operations
	cells, err := table.GetAllRows()
	if err != nil {
		return nil, err
	}

	// Convert cells to rows (business logic)
	rows := make([]*Row, len(cells))
	for i, cell := range cells {
		row, err := db.cellToRow(cell)
		if err != nil {
			return nil, NewDatabaseError("convert_cell_to_row", err, map[string]interface{}{
				"row_index": i,
			})
		}
		rows[i] = row
	}

	return rows, nil
}

// GetColumnValues returns all values for a specific column
func (db *SQLiteDB) GetColumnValues(tableName string, columnName string) ([]Value, error) {
	schema, err := db.GetTableSchema(tableName)
	if err != nil {
		return nil, err
	}

	columnIndex := -1
	for _, col := range schema {
		if strings.EqualFold(col.Name, columnName) {
			columnIndex = col.Index
			break
		}
	}

	if columnIndex == -1 {
		return nil, NewDatabaseError("find_column", ErrColumnNotFound, map[string]interface{}{
			"column_name": columnName,
			"table_name":  tableName,
		})
	}

	rows, err := db.GetTableRows(tableName)
	if err != nil {
		return nil, err
	}

	values := make([]Value, len(rows))
	for i, row := range rows {
		value, err := row.Get(columnIndex)
		if err != nil {
			return nil, NewDatabaseError("get_column_value_from_row", err, map[string]interface{}{
				"row_index":    i,
				"column_index": columnIndex,
			})
		}
		values[i] = value
	}

	return values, nil
}

// GetRowCount returns the number of rows in a table
func (db *SQLiteDB) GetRowCount(tableName string) (int, error) {
	table := db.GetTable(tableName)
	if table == nil {
		return 0, NewDatabaseError("get_row_count", ErrTableNotFound, map[string]interface{}{
			"table_name": tableName,
		})
	}

	return table.GetRowCount()
}

// cellToRow converts a cell to a row
func (db *SQLiteDB) cellToRow(cell *Cell) (*Row, error) {
	values := make([]Value, len(cell.Record.RecordBody.Values))
	for i, val := range cell.Record.RecordBody.Values {
		if val == nil {
			values[i] = NewSQLiteValue(0, nil) // NULL value
		} else if bytes, ok := val.([]byte); ok {
			// Determine serial type from the header
			if i < len(cell.Record.RecordHeader.SerialTypes) {
				serialType := cell.Record.RecordHeader.SerialTypes[i]
				values[i] = NewSQLiteValue(serialType, bytes)
			} else {
				values[i] = NewSQLiteValue(12, bytes) // Default to BLOB
			}
		} else {
			// Handle other value types
			values[i] = NewSQLiteValue(12, []byte(fmt.Sprintf("%v", val)))
		}
	}

	return &Row{Values: values}, nil
}

// parseTableSchema parses table schema from CREATE TABLE SQL
func (db *SQLiteDB) parseTableSchema(schemaSQL string) ([]*Column, error) {
	// Normalize SQLite syntax to MySQL syntax for sqlparser
	normalizedSQL := normalizeSQLiteToMySQL(schemaSQL)

	// Try to parse with sqlparser
	stmt, err := sqlparser.Parse(normalizedSQL)
	if err != nil {
		return nil, NewDatabaseError("parse_schema_sql", err, map[string]interface{}{
			"schema_sql":     schemaSQL,
			"normalized_sql": normalizedSQL,
		})
	}

	switch parsedStmt := stmt.(type) {
	case *sqlparser.DDL:
		if parsedStmt.Action != "create" || parsedStmt.TableSpec == nil {
			return nil, NewDatabaseError("invalid_ddl_statement", ErrInvalidDatabase, map[string]interface{}{
				"action": parsedStmt.Action,
			})
		}

		columns := make([]*Column, len(parsedStmt.TableSpec.Columns))
		for i, col := range parsedStmt.TableSpec.Columns {
			columns[i] = &Column{
				Name:     col.Name.String(),
				Type:     col.Type.Type,
				Index:    i,
				Nullable: true, // Default assumption
			}
		}

		return columns, nil

	default:
		return nil, NewDatabaseError("unsupported_schema_statement", ErrInvalidDatabase, map[string]interface{}{
			"statement_type": parsedStmt,
		})
	}
}

// normalizeSQLiteToMySQL converts SQLite-specific syntax to MySQL syntax for sqlparser
func normalizeSQLiteToMySQL(sql string) string {
	// Fix MySQL syntax: "primary key autoincrement" should be "AUTO_INCREMENT PRIMARY KEY"
	normalized := strings.ReplaceAll(sql, "primary key autoincrement", "AUTO_INCREMENT PRIMARY KEY")
	normalized = strings.ReplaceAll(normalized, "PRIMARY KEY AUTOINCREMENT", "AUTO_INCREMENT PRIMARY KEY")
	return normalized
}
