package main

import (
	"bytes"
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
func (tr *TableRawImpl) ReadAllCells(ctx context.Context) ([]CellWithPosition, error) {
	// Read the table's root page
	pageData, err := tr.dbRaw.ReadPage(ctx, tr.rootPage)
	if err != nil {
		return nil, fmt.Errorf("read table page %d for table %s: %w",
			tr.rootPage, tr.name, err)
	}

	// Parse page header first
	pageHeader, err := tr.parsePageHeader(pageData)
	if err != nil {
		return nil, fmt.Errorf("parse page header for table %s: %w", tr.name, err)
	}

	return tr.readCellsFromPage(ctx, pageHeader, pageData, 0, 1) // Start at page 0, with rowId starting at 1
}

// GetRootPage returns the root page number
func (tr *TableRawImpl) GetRootPage() int {
	return tr.rootPage
}

// GetName returns the table name
func (tr *TableRawImpl) GetName() string {
	return tr.name
}

// parsePageHeader parses a page header from page data
func (tr *TableRawImpl) parsePageHeader(pageData []byte) (*PageHeader, error) {
	if len(pageData) < 8 {
		return nil, fmt.Errorf("page data too small for header: have %d bytes, need at least 8", len(pageData))
	}

	// For table pages, header starts at offset 0 (unlike database page 1 which has db header first)
	headerReader := bytes.NewReader(pageData[:8])

	header := &PageHeader{}
	if err := binary.Read(headerReader, binary.BigEndian, header); err != nil {
		return nil, fmt.Errorf("failed to read page header: %w", err)
	}

	return header, nil
}

// readCellsFromPage reads all cells from a page with context support
func (tr *TableRawImpl) readCellsFromPage(ctx context.Context, pageHeader *PageHeader, pageData []byte, pageNum int, startRowId uint64) ([]CellWithPosition, error) {
	// Check context before starting work
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("read cells cancelled: %w", err)
	}

	if len(pageData) < 8 {
		return nil, fmt.Errorf("page too small for table %s: have %d bytes, need at least 8",
			tr.name, len(pageData))
	}

	// Handle different page types
	if pageHeader.IsLeafTable() {
		return tr.readCellsFromLeafPage(ctx, pageHeader, pageData, pageNum, startRowId)
	} else if pageHeader.IsInteriorTable() {
		return tr.readCellsFromInteriorPage(ctx, pageHeader, pageData, pageNum, startRowId)
	} else {
		return nil, fmt.Errorf("unexpected page type for table %s: expected leaf or interior table, got 0x%02X",
			tr.name, pageHeader.PageType)
	}
}

// readCellsFromLeafPage reads all cells from a leaf table page
func (tr *TableRawImpl) readCellsFromLeafPage(ctx context.Context, pageHeader *PageHeader, pageData []byte, pageNum int, startRowId uint64) ([]CellWithPosition, error) {
	// Use parsed header fields
	cellCount := pageHeader.CellCount

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
	cells := make([]CellWithPosition, cellCount)
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

			// Create CellWithPosition, using the index in the pointer array as position
			// The 'id' will be startRowId + position for auto-increment behavior
			cells[index] = CellWithPosition{
				Cell:       *cell,
				Position:   index,
				PageNumber: pageNum,
				StartRowId: startRowId,
			}
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

// readCellsFromInteriorPage reads all cells from an interior table page by traversing child pages
func (tr *TableRawImpl) readCellsFromInteriorPage(ctx context.Context, pageHeader *PageHeader, pageData []byte, pageNum int, startRowId uint64) ([]CellWithPosition, error) {
	// Check context before starting work
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("read interior cells cancelled: %w", err)
	}

	if len(pageData) < 12 { // Interior pages have additional rightmost pointer (4 bytes)
		return nil, fmt.Errorf("page too small for interior table %s: have %d bytes, need at least 12",
			tr.name, len(pageData))
	}

	// Use parsed header fields
	cellCount := pageHeader.CellCount

	// Read rightmost child pointer (bytes 8-11 for interior pages)
	rightmostChild := binary.BigEndian.Uint32(pageData[8:12])
	// fmt.Fprintf(os.Stdout, "++Rightmost child page: 0x%x\n", rightmostChild)

	// Read cell pointer array (starts at byte 12 for interior pages)
	cellPointers := make([]CellPointer, cellCount)
	for i := uint16(0); i < cellCount; i++ {
		offset := 12 + i*2
		if int(offset+1) >= len(pageData) {
			return nil, fmt.Errorf("cell pointer array overflow for table %s at offset %d",
				tr.name, offset)
		}
		cellPointers[i] = CellPointer(binary.BigEndian.Uint16(pageData[offset : offset+2]))
	}

	// For interior pages, we need to traverse child pages to collect actual cells
	// Each interior cell contains a child page number and a key
	var allCells []CellWithPosition
	currentRowId := startRowId

	// First, process all child pages referenced by interior cells
	for _, pointer := range cellPointers {
		// Read the interior cell to get child page number
		childPageNum, err := tr.readInteriorCellChildPage(pageData, int(pointer.Offset()))
		if err != nil {
			return nil, fmt.Errorf("read interior cell child page for table %s: %w", tr.name, err)
		}

		// Recursively read cells from child page
		childCells, err := tr.readCellsFromChildPage(ctx, int(childPageNum), currentRowId)
		if err != nil {
			return nil, fmt.Errorf("read cells from child page %d for table %s: %w", childPageNum, tr.name, err)
		}

		allCells = append(allCells, childCells...)
		// Update currentRowId for the next page based on number of cells read
		currentRowId += uint64(len(childCells))
	}

	// Finally, process the rightmost child page
	rightmostCells, err := tr.readCellsFromChildPage(ctx, int(rightmostChild), currentRowId)
	if err != nil {
		return nil, fmt.Errorf("read cells from rightmost child page %d for table %s: %w", rightmostChild, tr.name, err)
	}

	allCells = append(allCells, rightmostCells...)

	return allCells, nil
}

// readInteriorCellChildPage reads the child page number from an interior table cell
func (tr *TableRawImpl) readInteriorCellChildPage(pageData []byte, offset int) (uint32, error) {
	if offset+4 > len(pageData) {
		return 0, fmt.Errorf("interior cell offset %d exceeds page size %d", offset, len(pageData))
	}

	// Interior table cell format: 4-byte child page number + varint key
	childPageNum := binary.BigEndian.Uint32(pageData[offset : offset+4])
	return childPageNum, nil
}

// readCellsFromChildPage recursively reads cells from a child page
func (tr *TableRawImpl) readCellsFromChildPage(ctx context.Context, pageNum int, startRowId uint64) ([]CellWithPosition, error) {
	// Check context before recursive call
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("read child page cancelled: %w", err)
	}

	// Read the child page
	pageData, err := tr.dbRaw.ReadPage(ctx, pageNum)
	if err != nil {
		return nil, fmt.Errorf("read child page %d for table %s: %w", pageNum, tr.name, err)
	}

	// Parse child page header
	childPageHeader, err := tr.parsePageHeader(pageData)
	if err != nil {
		return nil, fmt.Errorf("parse child page header for page %d: %w", pageNum, err)
	}

	// Recursively process the child page
	return tr.readCellsFromPage(ctx, childPageHeader, pageData, pageNum, startRowId)
}
