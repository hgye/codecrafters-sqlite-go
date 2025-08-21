package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

// DatabaseRawImpl implements DatabaseRawInterface
type DatabaseRawImpl struct {
	file     *os.File
	header   *DatabaseHeader
	pageSize int
}

// NewDatabaseRaw creates a new raw database instance
func NewDatabaseRaw(filePath string) (*DatabaseRawImpl, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, NewDatabaseError("open_database_file", err, map[string]interface{}{
			"file_path": filePath,
		})
	}

	db := &DatabaseRawImpl{
		file: file,
	}

	// Parse the database header
	if err := db.parseHeader(); err != nil {
		file.Close()
		return nil, NewDatabaseError("parse_database_header", err, nil)
	}

	return db, nil
}

// ReadPage reads a page from the database file
func (db *DatabaseRawImpl) ReadPage(pageNum int) ([]byte, error) {

	// Offset start at (pageNum-1) * pageSize
	offset := int64(pageNum-1) * int64(db.pageSize)

	pageData := make([]byte, db.pageSize)
	n, err := db.file.ReadAt(pageData, offset)
	if err != nil {
		return nil, NewDatabaseError("read_page", err, map[string]interface{}{
			"page_num": pageNum,
			"offset":   offset,
		})
	}
	if n != db.pageSize {
		return nil, NewDatabaseError("read_page", fmt.Errorf("incomplete page read"), map[string]interface{}{
			"page_num":      pageNum,
			"expected_size": db.pageSize,
			"actual_size":   n,
		})
	}

	return pageData, nil
}

// ReadSchemaTable reads the schema table (sqlite_schema/sqlite_master) from page 1
func (db *DatabaseRawImpl) ReadSchemaTable() ([]Cell, error) {
	// Schema table is always on page 1
	pageData, err := db.ReadPage(1)
	if err != nil {
		return nil, err
	}

	// For page 1, the page header starts at offset 100, but cell offsets are relative to page start (offset 0)
	return db.readCellsFromPage1(pageData)
}

// GetPageSize returns the database page size
func (db *DatabaseRawImpl) GetPageSize() int {
	return db.pageSize
}

// GetHeader returns the database header for inspection
func (db *DatabaseRawImpl) GetHeader() *DatabaseHeader {
	return db.header
}

// Close closes the database file
func (db *DatabaseRawImpl) Close() error {
	if db.file != nil {
		return db.file.Close()
	}
	return nil
}

// parseHeader parses the 100-byte database header using Go's binary package
func (db *DatabaseRawImpl) parseHeader() error {
	// Seek to the beginning of the file
	if _, err := db.file.Seek(0, io.SeekStart); err != nil {
		return NewDatabaseError("seek_header", err, nil)
	}

	// Create a new header instance
	db.header = &DatabaseHeader{}

	// Use binary.Read to parse the header in a structured way
	if err := binary.Read(db.file, binary.BigEndian, db.header); err != nil {
		return NewDatabaseError("read_header", err, nil)
	}

	// Validate magic number using the new method
	if !db.header.IsValidMagicNumber() {
		return NewDatabaseError("invalid_magic", fmt.Errorf("invalid SQLite magic number"), map[string]interface{}{
			"expected": "SQLite format 3",
			"actual":   string(db.header.MagicNumber[:15]), // Exclude null terminator
		})
	}

	// Set page size using the helper method
	db.pageSize = db.header.GetActualPageSize()

	// Validate page size
	if db.pageSize < 512 || db.pageSize > 65536 || (db.pageSize&(db.pageSize-1)) != 0 {
		return NewDatabaseError("invalid_page_size", fmt.Errorf("invalid page size"), map[string]interface{}{
			"page_size": db.pageSize,
		})
	}

	return nil
}

// readCellsFromPage1 reads all cells from page 1 using structured parsing
func (db *DatabaseRawImpl) readCellsFromPage1(pageData []byte) ([]Cell, error) {
	// Page header starts at offset 100 (after database header)
	const headerOffset = 100

	if len(pageData) < headerOffset+8 {
		return nil, NewDatabaseError("read_cells", fmt.Errorf("page too small for page 1"), map[string]interface{}{
			"page_size": len(pageData),
		})
	}

	// Parse page header using a reader starting at the correct offset
	headerReader := bytes.NewReader(pageData[headerOffset:])
	pageHeader, err := db.parsePageHeader(headerReader)
	if err != nil {
		return nil, NewDatabaseError("parse_page_header", err, nil)
	}

	// Validate page type using the new helper method
	if !pageHeader.IsLeafTable() {
		return nil, NewDatabaseError("read_cells", fmt.Errorf("unexpected page type"), map[string]interface{}{
			"expected": "0x0D (leaf table)",
			"actual":   fmt.Sprintf("0x%02X", pageHeader.PageType),
		})
	}

	// Read cell pointer array using structured approach
	cellPointers, err := db.readCellPointers(headerReader, int(pageHeader.CellCount))
	if err != nil {
		return nil, NewDatabaseError("read_cell_pointers", err, nil)
	}

	// Read cells in parallel - NOTE: cell offsets are relative to the start of the page (offset 0)
	cells := make([]Cell, pageHeader.CellCount)
	errors := make([]error, pageHeader.CellCount)
	var wg sync.WaitGroup

	// Launch a goroutine for each cell
	for i, pointer := range cellPointers {
		wg.Add(1)
		go func(index int, cellPointer CellPointer) {
			defer wg.Done()

			cell, err := db.readCell(pageData, int(cellPointer.Offset()))
			if err != nil {
				errors[index] = NewDatabaseError("read_cell", err, map[string]interface{}{
					"cell_index": index,
					"offset":     cellPointer.Offset(),
				})
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
