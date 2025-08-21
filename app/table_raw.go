package main

import (
	"encoding/binary"
	"fmt"
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

// ReadAllCells reads all cells from the table's root page
func (tr *TableRawImpl) ReadAllCells() ([]Cell, error) {
	// Read the table's root page
	pageData, err := tr.dbRaw.ReadPage(tr.rootPage)
	if err != nil {
		return nil, NewDatabaseError("read_table_page", err, map[string]interface{}{
			"table":     tr.name,
			"root_page": tr.rootPage,
		})
	}

	return tr.readCellsFromPage(pageData)
}

// GetRootPage returns the root page number
func (tr *TableRawImpl) GetRootPage() int {
	return tr.rootPage
}

// GetName returns the table name
func (tr *TableRawImpl) GetName() string {
	return tr.name
}

// readCellsFromPage reads all cells from a page
func (tr *TableRawImpl) readCellsFromPage(pageData []byte) ([]Cell, error) {
	if len(pageData) < 8 {
		return nil, NewDatabaseError("read_table_cells", fmt.Errorf("page too small"), map[string]interface{}{
			"table":     tr.name,
			"page_size": len(pageData),
		})
	}

	// Parse page header
	pageType := pageData[0]
	if pageType != 0x0D { // Leaf table b-tree page
		return nil, NewDatabaseError("read_table_cells", fmt.Errorf("unexpected page type"), map[string]interface{}{
			"table":    tr.name,
			"expected": "0x0D (leaf table)",
			"actual":   fmt.Sprintf("0x%02X", pageType),
		})
	}

	// Read cell count (bytes 3-4)
	cellCount := binary.BigEndian.Uint16(pageData[3:5])

	// Read cell pointer array (starts at byte 8)
	cellPointers := make([]CellPointer, cellCount)
	for i := uint16(0); i < cellCount; i++ {
		offset := 8 + i*2
		if int(offset+1) >= len(pageData) {
			return nil, NewDatabaseError("read_table_cell_pointers", fmt.Errorf("cell pointer array overflow"), map[string]interface{}{
				"table": tr.name,
			})
		}
		cellPointers[i] = CellPointer(binary.BigEndian.Uint16(pageData[offset : offset+2]))
	}

	// Read cells
	cells := make([]Cell, cellCount)
	for i, pointer := range cellPointers {
		cell, err := tr.readCell(pageData, int(pointer.Offset()))
		if err != nil {
			return nil, NewDatabaseError("read_table_cell", err, map[string]interface{}{
				"table":      tr.name,
				"cell_index": i,
				"offset":     pointer.Offset(),
			})
		}
		cells[i] = *cell
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
