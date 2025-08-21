package main

// Logical layer interfaces - clean user-facing API

// Database represents a logical database with high-level operations
type Database interface {
	GetSchema() ([]SchemaRecord, error)
	GetTable(name string) (Table, error)
	GetTables() ([]string, error)
	Close() error
}

// Table represents a logical table with user-friendly operations
type Table interface {
	GetSchema() ([]Column, error)
	GetRows() ([]Row, error)
	SelectColumns(columns []string) ([]Row, error)
	Filter(condition func(Row) bool) ([]Row, error)
	Count() (int, error)
	GetName() string
}

// Physical layer interfaces - handle SQLite file format

// DatabaseRaw handles raw SQLite file I/O operations
type DatabaseRaw interface {
	ReadPage(pageNum int) ([]byte, error)
	ReadSchemaTable() ([]Cell, error)
	GetPageSize() int
	Close() error
}

// TableRaw handles raw table data access from SQLite format
type TableRaw interface {
	ReadAllCells() ([]Cell, error)
	GetRootPage() int
	GetName() string
}

// Physical data structures from SQLite format

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

// Offset returns the uint16 offset value
func (cp CellPointer) Offset() uint16 {
	return uint16(cp)
}

// IsValid checks if the cell pointer is valid (non-zero)
func (cp CellPointer) IsValid() bool {
	return cp > 0
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

// Utility functions for SQLite data parsing

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
			return body, offset, NewDatabaseError("read_record_body", ErrInvalidDatabase, map[string]interface{}{
				"needed_bytes": offset + size,
				"have_bytes":   len(data),
			})
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
		}
	}
	if rb.Values[4] != nil {
		schema.SQL = string(rb.Values[4].([]byte))
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
