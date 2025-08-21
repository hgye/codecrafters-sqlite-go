package main

import (
	"encoding/binary"
	"fmt"
	"os"
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

// Close closes the database file
func (db *DatabaseRawImpl) Close() error {
	if db.file != nil {
		return db.file.Close()
	}
	return nil
}

// parseHeader parses the 100-byte database header
func (db *DatabaseRawImpl) parseHeader() error {
	headerBytes := make([]byte, 100)
	n, err := db.file.Read(headerBytes)
	if err != nil {
		return err
	}
	if n != 100 {
		return fmt.Errorf("incomplete header read: got %d bytes, expected 100", n)
	}

	db.header = &DatabaseHeader{}

	// Parse key fields
	copy(db.header.MagicNumber[:], headerBytes[0:16])
	db.header.PageSize = binary.BigEndian.Uint16(headerBytes[16:18])

	// Handle special case where page size is stored as 1 (means 65536)
	if db.header.PageSize == 1 {
		db.pageSize = 65536
	} else {
		db.pageSize = int(db.header.PageSize)
	}

	return nil
}

// readCellsFromPage1 reads all cells from page 1 (handles the database header offset)
func (db *DatabaseRawImpl) readCellsFromPage1(pageData []byte) ([]Cell, error) {
	// Page header starts at offset 100 (after database header)
	headerOffset := 100
	if len(pageData) < headerOffset+8 {
		return nil, NewDatabaseError("read_cells", fmt.Errorf("page too small for page 1"), map[string]interface{}{
			"page_size": len(pageData),
		})
	}

	// Parse page header (starting at offset 100)
	pageType := pageData[headerOffset]
	if pageType != 0x0D { // Leaf table b-tree page
		return nil, NewDatabaseError("read_cells", fmt.Errorf("unexpected page type"), map[string]interface{}{
			"expected": "0x0D (leaf table)",
			"actual":   fmt.Sprintf("0x%02X", pageType),
		})
	}

	// Read cell count (bytes 103-104, which is headerOffset+3 to headerOffset+5)
	cellCount := binary.BigEndian.Uint16(pageData[headerOffset+3 : headerOffset+5])

	// Read cell pointer array (starts at byte 108, which is headerOffset+8)
	cellPointers := make([]CellPointer, cellCount)
	for i := uint16(0); i < cellCount; i++ {
		offset := headerOffset + 8 + int(i)*2
		if offset+1 >= len(pageData) {
			return nil, NewDatabaseError("read_cell_pointers", fmt.Errorf("cell pointer array overflow"), nil)
		}
		cellPointers[i] = CellPointer(binary.BigEndian.Uint16(pageData[offset : offset+2]))
	}

	// Read cells - NOTE: cell offsets are relative to the start of the page (offset 0), not headerOffset
	cells := make([]Cell, cellCount)
	for i, pointer := range cellPointers {
		cell, err := db.readCell(pageData, int(pointer.Offset()))
		if err != nil {
			return nil, NewDatabaseError("read_cell", err, map[string]interface{}{
				"cell_index": i,
				"offset":     pointer.Offset(),
			})
		}
		cells[i] = *cell
	}

	return cells, nil
}

// readCell reads a cell from page data at the given offset
func (db *DatabaseRawImpl) readCell(pageData []byte, offset int) (*Cell, error) {
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
		return nil, fmt.Errorf("payload extends beyond page boundary")
	}
	payload := pageData[offset : offset+int(payloadSize)]

	// Parse record from payload
	record, err := db.parseRecord(payload)
	if err != nil {
		return nil, err
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
