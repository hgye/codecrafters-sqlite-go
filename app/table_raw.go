package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
)

// TableRawImpl implements TableRawInterface for raw SQLite table operations
type TableRawImpl struct {
	dbRaw    DatabaseRaw
	name     string
	rootPage int
}

// NewTableRaw creates a new raw table instance
func NewTableRaw(dbRaw DatabaseRaw, name string, rootPage int) *TableRawImpl {
	return &TableRawImpl{
		dbRaw:    dbRaw,
		name:     name,
		rootPage: rootPage,
	}
}

// ReadAllCells reads all cells from the table's root page with context
func (tr *TableRawImpl) ReadAllCells(ctx context.Context) ([]Cell, error) {
	// Read the table's root page
	pageData, err := tr.dbRaw.ReadPage(ctx, tr.rootPage)
	if err != nil {
		return nil, fmt.Errorf("read table page %d for table %s: %w",
			tr.rootPage, tr.name, err)
	}

	return tr.readCellsFromPage(ctx, pageData)
}

// GetRootPage returns the root page number
func (tr *TableRawImpl) GetRootPage() int {
	return tr.rootPage
}

// GetName returns the table name
func (tr *TableRawImpl) GetName() string {
	return tr.name
}

// readCellsFromPage reads all cells from a page with context support
func (tr *TableRawImpl) readCellsFromPage(ctx context.Context, pageData []byte) ([]Cell, error) {
	// Check context before starting work
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("read cells cancelled: %w", err)
	}

	if len(pageData) < 8 {
		return nil, fmt.Errorf("page too small for table %s: have %d bytes, need at least 8",
			tr.name, len(pageData))
	}

	// Parse page header
	pageType := pageData[0]
	if pageType != 0x0D { // Leaf table b-tree page
		return nil, fmt.Errorf("unexpected page type for table %s: expected 0x0D (leaf table), got 0x%02X",
			tr.name, pageType)
	}

	// Read cell count (bytes 3-4)
	cellCount := binary.BigEndian.Uint16(pageData[3:5])

	// Read cell pointer array (starts at byte 8)
	cellPointers := make([]CellPointer, cellCount)
	for i := uint16(0); i < cellCount; i++ {
		offset := 8 + i*2
		if int(offset+1) >= len(pageData) {
			return nil, fmt.Errorf("cell pointer array overflow for table %s at offset %d",
				tr.name, offset)
		}
		cellPointers[i] = CellPointer(binary.BigEndian.Uint16(pageData[offset : offset+2]))
	}

	// Read cells in parallel with context support
	cells := make([]Cell, cellCount)
	errors := make([]error, cellCount)
	var wg sync.WaitGroup

	// Launch a goroutine for each cell
	for i, pointer := range cellPointers {
		wg.Add(1)
		go func(index int, cellPointer CellPointer) {
			defer wg.Done()

			// Check context in goroutine
			select {
			case <-ctx.Done():
				errors[index] = fmt.Errorf("cell read cancelled for table %s: %w", tr.name, ctx.Err())
				return
			default:
			}

			cell, err := tr.readCell(pageData, int(cellPointer.Offset()))
			if err != nil {
				errors[index] = fmt.Errorf("read cell %d at offset %d for table %s: %w",
					index, cellPointer.Offset(), tr.name, err)
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

// readCell reads a cell from page data at the given offset
func (tr *TableRawImpl) readCell(pageData []byte, offset int) (*Cell, error) {
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
	record, err := tr.parseRecord(payload)
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
func (tr *TableRawImpl) parseRecord(payload []byte) (*Record, error) {
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
