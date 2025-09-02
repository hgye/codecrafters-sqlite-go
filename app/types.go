package main

import (
	"context"
	"fmt"
	"io"
)

// Logical layer interfaces - clean user-facing API

// Database represents a logical database with high-level operations
type Database interface {
	SchemaReader
	TableProvider
	IndexProvider
	io.Closer
	GetPageSize() int
}

// SchemaReader provides schema reading capabilities
type SchemaReader interface {
	GetSchema(ctx context.Context) ([]SchemaRecord, error)
	GetTables(ctx context.Context) ([]string, error)
}

// TableProvider provides table access capabilities
type TableProvider interface {
	GetTable(ctx context.Context, name string) (Table, error)
}

// IndexProvider provides index access capabilities
type IndexProvider interface {
	GetIndex(ctx context.Context, name string) (Index, error)
	GetIndices(ctx context.Context) ([]string, error)
	GetTableIndexes(ctx context.Context, tableName string) ([]Index, error)
}

// Table represents a logical table with user-friendly operations
type Table interface {
	SchemaProvider
	DataReader
	DataFilter
	GetName() string
	GetIndexes(ctx context.Context) ([]Index, error)
	GetIndexByName(name string) (Index, bool)
	GetRowByRowid(ctx context.Context, rowid int64) (*Row, error)
}

// Index represents a logical index with user-friendly operations
type Index interface {
	SchemaProvider
	DataReader
	GetName() string
	GetTableName() string
	SearchByKey(ctx context.Context, key interface{}) ([]IndexEntry, error)
}

// SchemaProvider provides schema information
type SchemaProvider interface {
	GetSchema(ctx context.Context) ([]Column, error)
}

// DataReader provides data reading capabilities
type DataReader interface {
	GetRows(ctx context.Context) ([]Row, error)
	Count(ctx context.Context) (int, error)
}

// DataFilter provides data filtering capabilities
type DataFilter interface {
	SelectColumns(ctx context.Context, columns []string) ([]Row, error)
	Filter(ctx context.Context, condition func(Row) bool) ([]Row, error)
}

// Physical layer interfaces - handle SQLite file format

// DatabaseRaw handles raw SQLite file I/O operations
type DatabaseRaw interface {
	PageReader
	SchemaTableReader
	io.Closer
}

// PageReader provides page reading capabilities
type PageReader interface {
	ReadPage(ctx context.Context, pageNum int) ([]byte, error)
	GetPageSize() int
}

// SchemaTableReader provides schema table reading capabilities
type SchemaTableReader interface {
	ReadSchemaTable(ctx context.Context) ([]Cell, error)
}

// TableRaw handles raw table data access from SQLite format
type TableRaw interface {
	CellReader
	GetRootPage() int
	GetName() string
	ReadCellByRowid(ctx context.Context, rowid int64) (*Cell, error)
}

// IndexRaw handles raw index data access from SQLite format
type IndexRaw interface {
	CellReader
	GetRootPage() int
	GetName() string
	// Index-specific methods
	SearchKeys(ctx context.Context, key interface{}) ([]IndexEntry, error)
	GetIndexedColumns() []string
}

// CellReader provides cell reading capabilities
type CellReader interface {
	ReadAllCells(ctx context.Context) ([]Cell, error)
}

// Configuration and Options

// DatabaseConfig holds database configuration options
type DatabaseConfig struct {
	PageCacheSize   int
	MaxConcurrency  int
	ReadTimeout     int // milliseconds
	ValidationMode  ValidationLevel
	EnableProfiling bool
}

// ValidationLevel defines validation strictness
type ValidationLevel int

const (
	ValidationNone ValidationLevel = iota
	ValidationBasic
	ValidationStrict
)

// DatabaseOption represents a functional option for database configuration
type DatabaseOption func(*DatabaseConfig)

// WithPageCacheSize sets the page cache size
func WithPageCacheSize(size int) DatabaseOption {
	return func(cfg *DatabaseConfig) {
		cfg.PageCacheSize = size
	}
}

// WithMaxConcurrency sets the maximum number of concurrent operations
func WithMaxConcurrency(max int) DatabaseOption {
	return func(cfg *DatabaseConfig) {
		cfg.MaxConcurrency = max
	}
}

// WithReadTimeout sets the read timeout in milliseconds
func WithReadTimeout(timeout int) DatabaseOption {
	return func(cfg *DatabaseConfig) {
		cfg.ReadTimeout = timeout
	}
}

// WithValidation sets the validation level
func WithValidation(level ValidationLevel) DatabaseOption {
	return func(cfg *DatabaseConfig) {
		cfg.ValidationMode = level
	}
}

// WithProfiling enables or disables profiling
func WithProfiling(enabled bool) DatabaseOption {
	return func(cfg *DatabaseConfig) {
		cfg.EnableProfiling = enabled
	}
}

// DefaultDatabaseConfig returns the default configuration
func DefaultDatabaseConfig() *DatabaseConfig {
	return &DatabaseConfig{
		PageCacheSize:   100,
		MaxConcurrency:  10,
		ReadTimeout:     5000, // 5 seconds
		ValidationMode:  ValidationBasic,
		EnableProfiling: false,
	}
}

// Resource Management

// ResourceManager handles cleanup of multiple resources
type ResourceManager struct {
	resources []io.Closer
	cleaners  []func() error
}

// NewResourceManager creates a new resource manager
func NewResourceManager() *ResourceManager {
	return &ResourceManager{
		resources: make([]io.Closer, 0),
		cleaners:  make([]func() error, 0),
	}
}

// Add adds a closeable resource to be managed
func (rm *ResourceManager) Add(resource io.Closer) {
	rm.resources = append(rm.resources, resource)
}

// AddCleaner adds a custom cleanup function
func (rm *ResourceManager) AddCleaner(cleaner func() error) {
	rm.cleaners = append(rm.cleaners, cleaner)
}

// Close closes all managed resources in reverse order (LIFO)
func (rm *ResourceManager) Close() error {
	var lastErr error

	// Run custom cleaners first (LIFO)
	for i := len(rm.cleaners) - 1; i >= 0; i-- {
		if err := rm.cleaners[i](); err != nil {
			lastErr = err
		}
	}

	// Close resources (LIFO)
	for i := len(rm.resources) - 1; i >= 0; i-- {
		if err := rm.resources[i].Close(); err != nil {
			lastErr = err
		}
	}

	return lastErr
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
	HeaderSize  uint64   // varint: total bytes in header including this varint
	SerialTypes []uint64 // serial types: one per column, determines datatype (can be large varints)
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
// Fields are ordered and sized according to the SQLite specification
type DatabaseHeader struct {
	MagicNumber     [16]byte // Offset 0: The magic number "SQLite format 3\000"
	PageSize        uint16   // Offset 16: Database page size in bytes
	FileFormatWrite uint8    // Offset 18: File format write version
	FileFormatRead  uint8    // Offset 19: File format read version
	ReservedBytes   uint8    // Offset 20: Bytes of unused "reserved" space at the end of each page
	MaxPayload      uint8    // Offset 21: Maximum embedded payload fraction (must be 64)
	MinPayload      uint8    // Offset 22: Minimum embedded payload fraction (must be 32)
	LeafPayload     uint8    // Offset 23: Leaf payload fraction (must be 32)
	FileChangeCount uint32   // Offset 24: File change counter
	DatabaseSize    uint32   // Offset 28: Size of the database file in pages
	FirstFreePage   uint32   // Offset 32: Page number of the first freelist trunk page
	FreePageCount   uint32   // Offset 36: Total number of freelist pages
	SchemaCookie    uint32   // Offset 40: The schema cookie
	SchemaFormat    uint32   // Offset 44: The schema format number (1, 2, 3, or 4)
	DefaultCache    uint32   // Offset 48: Default page cache size
	LargestBTree    uint32   // Offset 52: Page number of the largest root b-tree page when in auto-vacuum or incremental-vacuum modes
	TextEncoding    uint32   // Offset 56: Database text encoding (1=UTF-8, 2=UTF-16le, 3=UTF-16be)
	UserVersion     uint32   // Offset 60: The user version as set by PRAGMA user_version
	IncrVacuum      uint32   // Offset 64: True (non-zero) for incremental-vacuum mode. False (zero) otherwise.
	AppID           uint32   // Offset 68: The application ID set by PRAGMA application_id
	Reserved        [20]byte // Offset 72: Reserved for expansion. Must be zero.
	VersionValid    uint32   // Offset 92: The version-valid-for number
	SQLiteVersion   uint32   // Offset 96: SQLITE_VERSION_NUMBER
}

// String returns a human-readable representation of the database header
func (dh *DatabaseHeader) String() string {
	return fmt.Sprintf("SQLite Database Header: PageSize=%d, TextEncoding=%d, SchemaFormat=%d, DatabaseSize=%d pages",
		dh.PageSize, dh.TextEncoding, dh.SchemaFormat, dh.DatabaseSize)
}

// GetActualPageSize returns the actual page size, handling the special case where 1 means 65536
func (dh *DatabaseHeader) GetActualPageSize() int {
	if dh.PageSize == 1 {
		return 65536
	}
	return int(dh.PageSize)
}

// IsValidMagicNumber checks if the magic number is valid
func (dh *DatabaseHeader) IsValidMagicNumber() bool {
	expected := [16]byte{'S', 'Q', 'L', 'i', 't', 'e', ' ', 'f', 'o', 'r', 'm', 'a', 't', ' ', '3', 0}
	return dh.MagicNumber == expected
}

// PageHeader represents a B-tree page header
// The exact structure depends on the page type, but this covers leaf table b-tree pages
type PageHeader struct {
	PageType         uint8  // Offset 0: Page type (0x0d for leaf table b-tree page)
	FirstFreeblock   uint16 // Offset 1: Byte offset of the first freeblock, or 0 if there are no freeblocks
	CellCount        uint16 // Offset 3: Number of cells on this page
	CellContentStart uint16 // Offset 5: Start of cell content area, or 0 if the page contains no cells
	FragmentedBytes  uint8  // Offset 7: Number of fragmented free bytes within the cell content area
	// RightmostPointer uint32 // Only for interior pages (not included for leaf pages)
}

// String returns a human-readable representation of the page header
func (ph *PageHeader) String() string {
	return fmt.Sprintf("Page Header: Type=0x%02X, CellCount=%d, CellContentStart=%d",
		ph.PageType, ph.CellCount, ph.CellContentStart)
}

// IsLeafTable checks if this is a leaf table b-tree page
func (ph *PageHeader) IsLeafTable() bool {
	return ph.PageType == 0x0D
}

// IsInteriorTable checks if this is an interior table b-tree page
func (ph *PageHeader) IsInteriorTable() bool {
	return ph.PageType == 0x05
}

// IsLeafIndex checks if this is a leaf index b-tree page
func (ph *PageHeader) IsLeafIndex() bool {
	return ph.PageType == 0x0A
}

// IsInteriorIndex checks if this is an interior index b-tree page
func (ph *PageHeader) IsInteriorIndex() bool {
	return ph.PageType == 0x02
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

// VarintReader provides structured varint reading capabilities
type VarintReader struct {
	data   []byte
	offset int
}

// NewVarintReader creates a new VarintReader
func NewVarintReader(data []byte) *VarintReader {
	return &VarintReader{data: data, offset: 0}
}

// ReadVarint reads a variable-length integer and advances the internal offset
func (vr *VarintReader) ReadVarint() (uint64, error) {
	if vr.offset >= len(vr.data) {
		return 0, fmt.Errorf("varint reader: offset %d exceeds data length %d", vr.offset, len(vr.data))
	}

	value, bytesRead := readVarint(vr.data, vr.offset)
	if bytesRead == 0 {
		return 0, fmt.Errorf("varint reader: invalid varint at offset %d", vr.offset)
	}

	vr.offset += bytesRead
	return value, nil
}

// Offset returns the current offset
func (vr *VarintReader) Offset() int {
	return vr.offset
}

// Remaining returns the number of bytes remaining
func (vr *VarintReader) Remaining() int {
	return len(vr.data) - vr.offset
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
func getSerialTypeSize(serialType uint64) int {
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

// readRecordHeader reads and parses a record header using structured approach
func readRecordHeader(data []byte, offset int) (RecordHeader, int) {
	var header RecordHeader
	reader := NewVarintReader(data[offset:])

	// Read header size
	headerSize, err := reader.ReadVarint()
	if err != nil {
		return header, offset // Return error case gracefully
	}
	header.HeaderSize = headerSize

	if header.HeaderSize == 0 {
		return header, offset + reader.Offset() // No header
	}

	// Calculate how many serial types we need to read
	headerEnd := int(header.HeaderSize)
	originalOffset := reader.Offset()

	for reader.Offset() < headerEnd && originalOffset < headerEnd {
		serialType, err := reader.ReadVarint()
		if err != nil {
			break // Error reading serial type
		}
		header.SerialTypes = append(header.SerialTypes, serialType)
	}

	return header, offset + reader.Offset()
}

// readRecordBody reads and parses a record body using structured approach
func readRecordBody(data []byte, offset int, header RecordHeader) (RecordBody, int, error) {
	var body RecordBody
	// Only create entries for serial types that actually store data (size > 0)
	var actualValues []interface{}

	currentOffset := offset
	for _, serialType := range header.SerialTypes {
		size := getSerialTypeSize(serialType)
		if size == 0 {
			// Serial type 0 means NULL/no data stored - skip entirely
			continue
		}
		if currentOffset+size > len(data) {
			return body, currentOffset, NewDatabaseError("read_record_body", ErrInvalidDatabase, map[string]interface{}{
				"needed_bytes": currentOffset + size,
				"have_bytes":   len(data),
			})
		}
		value := data[currentOffset : currentOffset+size]
		actualValues = append(actualValues, value) // Store raw bytes for now
		currentOffset += size
	}

	body.Values = actualValues
	return body, currentOffset, nil
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
