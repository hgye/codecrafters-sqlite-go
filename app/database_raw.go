package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
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

	// For page 1, the page header starts at offset 100, but cell offsets are relative to page start (offset 0)
	return db.readCellsFromPage1(ctx, pageData)
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

	// Parse page header using a reader starting at the correct offset
	headerReader := bytes.NewReader(pageData[headerOffset:])
	pageHeader, err := db.parsePageHeader(headerReader)
	if err != nil {
		return nil, fmt.Errorf("parse page header: %w", err)
	}

	// Validate page type using the new helper method
	if !pageHeader.IsLeafTable() {
		return nil, fmt.Errorf("unexpected page type: expected 0x0D (leaf table), got 0x%02X",
			pageHeader.PageType)
	}

	// Read cell pointer array using structured approach
	cellPointers, err := db.readCellPointers(headerReader, int(pageHeader.CellCount))
	if err != nil {
		return nil, fmt.Errorf("read cell pointers: %w", err)
	}

	// Read cells in parallel with context - NOTE: cell offsets are relative to the start of the page (offset 0)
	cells := make([]Cell, pageHeader.CellCount)
	errors := make([]error, pageHeader.CellCount)
	var wg sync.WaitGroup

	// Launch a goroutine for each cell
	for i, pointer := range cellPointers {
		wg.Add(1)
		go func(index int, cellPointer CellPointer) {
			defer wg.Done()

			// Check context in goroutine
			select {
			case <-ctx.Done():
				errors[index] = fmt.Errorf("cell read cancelled: %w", ctx.Err())
				return
			default:
			}

			cell, err := db.readCell(pageData, int(cellPointer.Offset()))
			if err != nil {
				errors[index] = fmt.Errorf("read cell %d at offset %d: %w",
					index, cellPointer.Offset(), err)
				return
			}
			cells[index] = *cell
		}(i, pointer)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Check for any errors
	for _, err := range errors {
		if err != nil {
			return nil, err
		}
	}

	return cells, nil
}

// parsePageHeader parses a B-tree page header using binary.Read
func (db *DatabaseRawImpl) parsePageHeader(reader io.Reader) (*PageHeader, error) {
	header := &PageHeader{}
	if err := binary.Read(reader, binary.BigEndian, header); err != nil {
		return nil, fmt.Errorf("failed to read page header: %w", err)
	}
	return header, nil
}

// readCellPointers reads cell pointers using structured parsing
func (db *DatabaseRawImpl) readCellPointers(reader io.Reader, cellCount int) ([]CellPointer, error) {
	cellPointers := make([]CellPointer, cellCount)
	for i := 0; i < cellCount; i++ {
		var pointer uint16
		if err := binary.Read(reader, binary.BigEndian, &pointer); err != nil {
			return nil, fmt.Errorf("failed to read cell pointer %d: %w", i, err)
		}
		cellPointers[i] = CellPointer(pointer)
	}
	return cellPointers, nil
}

// readCell reads a cell from page data at the given offset using structured parsing
func (db *DatabaseRawImpl) readCell(pageData []byte, offset int) (*Cell, error) {
	if offset >= len(pageData) {
		return nil, fmt.Errorf("cell offset %d exceeds page size %d", offset, len(pageData))
	}

	// Use VarintReader for structured varint parsing
	reader := NewVarintReader(pageData[offset:])

	// Read payload size (varint)
	payloadSize, err := reader.ReadVarint()
	if err != nil {
		return nil, fmt.Errorf("failed to read payload size: %w", err)
	}

	// Read row ID (varint)
	rowID, err := reader.ReadVarint()
	if err != nil {
		return nil, fmt.Errorf("failed to read row ID: %w", err)
	}

	// Calculate payload start offset
	payloadOffset := offset + reader.Offset()

	// Read payload
	if payloadOffset+int(payloadSize) > len(pageData) {
		return nil, fmt.Errorf("payload extends beyond page boundary: need %d bytes, have %d",
			payloadOffset+int(payloadSize), len(pageData))
	}
	payload := pageData[payloadOffset : payloadOffset+int(payloadSize)]

	// Parse record from payload
	record, err := db.parseRecord(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to parse record: %w", err)
	}

	return &Cell{
		PayloadSize: payloadSize,
		Rowid:       rowID,
		Record:      *record,
	}, nil
}

// parseRecord parses a record from payload data
func (db *DatabaseRawImpl) parseRecord(payload []byte) (*Record, error) {
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
