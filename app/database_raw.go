package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// DatabaseRawImpl implements DatabaseRawInterface with context support
type DatabaseRawImpl struct {
	file           *os.File
	header         *DatabaseHeader
	pageSize       int
	config         *DatabaseConfig
	resourceMgr    *ResourceManager
	concurrencySem chan struct{} // Semaphore for limiting concurrency
}

// NewDatabaseRaw creates a new raw database instance with functional options
func NewDatabaseRaw(filePath string, options ...DatabaseOption) (*DatabaseRawImpl, error) {
	// Apply configuration options
	config := DefaultDatabaseConfig()
	for _, opt := range options {
		opt(config)
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open database file: %w", err)
	}

	// Create resource manager
	resourceMgr := NewResourceManager()
	resourceMgr.Add(file)

	// Create concurrency semaphore
	concurrencySem := make(chan struct{}, config.MaxConcurrency)

	db := &DatabaseRawImpl{
		file:           file,
		config:         config,
		resourceMgr:    resourceMgr,
		concurrencySem: concurrencySem,
	}

	// Parse the database header
	if err := db.parseHeader(); err != nil {
		resourceMgr.Close()
		return nil, fmt.Errorf("parse database header: %w", err)
	}

	return db, nil
}

// ReadPage reads a page from the database file with context support
func (db *DatabaseRawImpl) ReadPage(ctx context.Context, pageNum int) ([]byte, error) {
	// Acquire concurrency semaphore
	select {
	case db.concurrencySem <- struct{}{}:
		defer func() { <-db.concurrencySem }()
	case <-ctx.Done():
		return nil, fmt.Errorf("read page cancelled: %w", ctx.Err())
	}

	// Check context before doing work
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("read page context error: %w", err)
	}

	// SQLite pages are 1-indexed, so page 1 is at offset 0
	offset := int64(pageNum-1) * int64(db.pageSize)
	// fmt.Printf("DEBUG: ReadPage(%d) - pageSize=%d, offset=0x%x\n", pageNum, db.pageSize, offset)

	pageData := make([]byte, db.pageSize)
	n, err := db.file.ReadAt(pageData, offset)
	if err != nil {
		return nil, fmt.Errorf("read page %d at offset %d: %w", pageNum, offset, err)
	}
	if n != db.pageSize {
		return nil, fmt.Errorf("incomplete page read: page %d, expected %d bytes, got %d",
			pageNum, db.pageSize, n)
	}

	return pageData, nil
}

// ReadSchemaTable reads the schema table (sqlite_schema/sqlite_master) from page 1 with context
func (db *DatabaseRawImpl) ReadSchemaTable(ctx context.Context) ([]Cell, error) {
	// Schema table is always on page 1
	pageData, err := db.ReadPage(ctx, 1)
	if err != nil {
		return nil, fmt.Errorf("read schema table page: %w", err)
	}

	// For page 1, we need special handling due to the database header
	// Use BTree for other pages, but use custom logic for page 1
	return db.readCellsFromPage1(ctx, pageData)
}

// readCellsFromPage1 reads all cells from page 1 using structured parsing with context
func (db *DatabaseRawImpl) readCellsFromPage1(ctx context.Context, pageData []byte) ([]Cell, error) {
	// Check context before starting work
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("read cells cancelled: %w", err)
	}

	// Page header starts at offset 100 (after database header)
	const headerOffset = 100

	if len(pageData) < headerOffset+8 {
		return nil, fmt.Errorf("page too small for page 1: have %d bytes, need at least %d",
			len(pageData), headerOffset+8)
	}

	// Parse page header starting at the correct offset
	pageHeader, err := db.parsePageHeaderAtOffset(pageData, headerOffset)
	if err != nil {
		return nil, fmt.Errorf("parse page header: %w", err)
	}

	// Validate page type
	if pageHeader.PageType != 0x0D {
		return nil, fmt.Errorf("unexpected page type: expected 0x0D (leaf table), got 0x%02X",
			pageHeader.PageType)
	}

	// Read cell pointers
	cellPointers, err := db.readCellPointersFromPage(pageData, headerOffset+8, int(pageHeader.CellCount))
	if err != nil {
		return nil, fmt.Errorf("read cell pointers: %w", err)
	}

	// Read cells - cell offsets are relative to the start of the page (offset 0)
	cells := make([]Cell, pageHeader.CellCount)
	for i, pointer := range cellPointers {
		cell, err := db.readCellAtOffset(pageData, int(pointer))
		if err != nil {
			return nil, fmt.Errorf("read cell %d at offset %d: %w", i, pointer, err)
		}
		cells[i] = *cell
	}

	return cells, nil
}

// parsePageHeaderAtOffset parses a page header at a specific offset
func (db *DatabaseRawImpl) parsePageHeaderAtOffset(pageData []byte, offset int) (*PageHeader, error) {
	if offset+8 > len(pageData) {
		return nil, fmt.Errorf("not enough data for page header at offset %d", offset)
	}

	return &PageHeader{
		PageType:         pageData[offset],
		FirstFreeblock:   binary.BigEndian.Uint16(pageData[offset+1:]),
		CellCount:        binary.BigEndian.Uint16(pageData[offset+3:]),
		CellContentStart: binary.BigEndian.Uint16(pageData[offset+5:]),
		FragmentedBytes:  pageData[offset+7],
	}, nil
}

// readCellPointersFromPage reads cell pointers starting at a specific offset
func (db *DatabaseRawImpl) readCellPointersFromPage(pageData []byte, offset int, cellCount int) ([]uint16, error) {
	cellPointers := make([]uint16, cellCount)
	for i := 0; i < cellCount; i++ {
		ptrOffset := offset + i*2
		if ptrOffset+2 > len(pageData) {
			return nil, fmt.Errorf("cell pointer %d extends beyond page", i)
		}
		cellPointers[i] = binary.BigEndian.Uint16(pageData[ptrOffset:])
	}
	return cellPointers, nil
}

// readCellAtOffset reads a cell at a specific offset
func (db *DatabaseRawImpl) readCellAtOffset(pageData []byte, offset int) (*Cell, error) {
	if offset >= len(pageData) {
		return nil, fmt.Errorf("cell offset %d exceeds page size %d", offset, len(pageData))
	}

	// Read payload size (varint)
	payloadSize, bytesRead := readVarint(pageData, offset)
	offset += bytesRead

	// Read row ID (varint)
	rowID, bytesRead := readVarint(pageData, offset)
	offset += bytesRead

	// Read payload
	if offset+int(payloadSize) > len(pageData) {
		return nil, fmt.Errorf("payload extends beyond page boundary: need %d bytes, have %d",
			offset+int(payloadSize), len(pageData))
	}
	payload := pageData[offset : offset+int(payloadSize)]

	// Parse record from payload
	header, headerOffset := readRecordHeader(payload, 0)
	body, _, err := readRecordBody(payload, headerOffset, header)
	if err != nil {
		return nil, fmt.Errorf("parse record body: %w", err)
	}

	return &Cell{
		PayloadSize: payloadSize,
		Rowid:       rowID,
		Record: Record{
			RecordHeader: header,
			RecordBody:   body,
		},
	}, nil
}

// GetPageSize returns the database page size
func (db *DatabaseRawImpl) GetPageSize() int {
	return db.pageSize
}

// GetHeader returns the database header for inspection
func (db *DatabaseRawImpl) GetHeader() *DatabaseHeader {
	return db.header
}

// Close closes the database file using resource manager
func (db *DatabaseRawImpl) Close() error {
	if db.resourceMgr != nil {
		return db.resourceMgr.Close()
	}
	return nil
}

// parseHeader parses the 100-byte database header using Go's binary package
func (db *DatabaseRawImpl) parseHeader() error {
	// Seek to the beginning of the file
	if _, err := db.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek to header: %w", err)
	}

	// Create a new header instance
	db.header = &DatabaseHeader{}

	// Use binary.Read to parse the header in a structured way
	if err := binary.Read(db.file, binary.BigEndian, db.header); err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	// Validate magic number using the new method
	if !db.header.IsValidMagicNumber() {
		return fmt.Errorf("invalid SQLite magic number: expected 'SQLite format 3', got '%s'",
			string(db.header.MagicNumber[:15]))
	}

	// Set page size using the helper method
	db.pageSize = db.header.GetActualPageSize()

	// Validate page size
	if db.pageSize < 512 || db.pageSize > 65536 || (db.pageSize&(db.pageSize-1)) != 0 {
		return fmt.Errorf("invalid page size: %d (must be power of 2 between 512 and 65536)",
			db.pageSize)
	}

	return nil
}
