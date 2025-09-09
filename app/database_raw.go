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
	// Schema table is always on page 1 - use BTree abstraction
	btree := NewBTree(db, 1, BTreeTypeTable)
	cells, err := btree.TraverseAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("read schema table: %w", err)
	}
	return cells, nil
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
