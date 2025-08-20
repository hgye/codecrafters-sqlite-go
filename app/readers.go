package main

import (
	"encoding/binary"
	"io"
)

// FileReader provides an abstraction over file I/O operations
type FileReader interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
}

// PageReader handles page-level operations
type PageReader struct {
	reader   FileReader
	pageSize uint16
}

// NewPageReader creates a new page reader
func NewPageReader(reader FileReader, pageSize uint16) *PageReader {
	return &PageReader{
		reader:   reader,
		pageSize: pageSize,
	}
}

// ReadPageHeader reads a page header at the specified page number
func (pr *PageReader) ReadPageHeader(pageNum uint32) (*PageHeader, error) {
	offset := pr.calculatePageOffset(pageNum)

	if _, err := pr.reader.Seek(offset, 0); err != nil {
		return nil, NewDatabaseError("seek_page_header", err, map[string]interface{}{
			"page_num": pageNum,
			"offset":   offset,
		})
	}

	var header PageHeader
	if err := binary.Read(pr.reader, binary.BigEndian, &header); err != nil {
		return nil, NewDatabaseError("read_page_header", err, map[string]interface{}{
			"page_num": pageNum,
		})
	}

	return &header, nil
}

// ReadCellPointers reads cell pointers from a page
func (pr *PageReader) ReadCellPointers(pageNum uint32, cellCount uint16) ([]CellPointer, error) {
	// Position after page header
	offset := pr.calculatePageOffset(pageNum) + int64(pageHeaderSize(pageNum))

	if _, err := pr.reader.Seek(offset, 0); err != nil {
		return nil, NewDatabaseError("seek_cell_pointers", err, map[string]interface{}{
			"page_num": pageNum,
		})
	}

	pointers := make([]uint16, cellCount)
	if err := binary.Read(pr.reader, binary.BigEndian, &pointers); err != nil {
		return nil, NewDatabaseError("read_cell_pointers", err, map[string]interface{}{
			"page_num":   pageNum,
			"cell_count": cellCount,
		})
	}

	// Convert and validate
	result := make([]CellPointer, cellCount)
	for i, ptr := range pointers {
		if ptr == 0 || ptr > uint16(pr.pageSize) {
			return nil, NewDatabaseError("invalid_cell_pointer", ErrInvalidCellPointer, map[string]interface{}{
				"pointer_index": i,
				"pointer_value": ptr,
				"page_size":     pr.pageSize,
			})
		}
		result[i] = CellPointer(ptr)
	}

	return result, nil
}

// calculatePageOffset calculates the byte offset for a page number
func (pr *PageReader) calculatePageOffset(pageNum uint32) int64 {
	if pageNum == 1 {
		return 100 // First page starts after 100-byte header
	}
	return int64((pageNum - 1) * uint32(pr.pageSize))
}

// pageHeaderSize returns the size of the page header based on page type
func pageHeaderSize(pageNum uint32) int {
	// This would need to be determined by reading the page type first
	// For now, assume leaf page header size
	return 8 // Standard B-tree leaf page header size
}

// VarintReader handles varint reading operations
type VarintReader struct {
	reader FileReader
}

// NewVarintReader creates a new varint reader
func NewVarintReader(reader FileReader) *VarintReader {
	return &VarintReader{reader: reader}
}

// ReadVarint reads a variable-length integer from the current position
func (vr *VarintReader) ReadVarint() (uint64, int, error) {
	var result uint64
	var buf [1]byte

	for i := 0; i < 9; i++ {
		n, err := vr.reader.ReadAt(buf[:], 0)
		if err != nil && err != io.EOF {
			return 0, 0, NewDatabaseError("read_varint_byte", err, map[string]interface{}{
				"byte_index": i,
			})
		}
		if n == 0 {
			return 0, 0, NewDatabaseError("read_varint_incomplete", ErrInsufficientData, nil)
		}

		b := buf[0]
		if i == 8 {
			// 9th byte uses all 8 bits
			result = (result << 8) | uint64(b)
			return result, i + 1, nil
		}

		// First 8 bytes use lower 7 bits
		result = (result << 7) | uint64(b&0x7F)
		if (b & 0x80) == 0 {
			// High bit clear means this is the last byte
			return result, i + 1, nil
		}

		// Advance position for next byte
		if _, err := vr.reader.Seek(1, 1); err != nil {
			return 0, 0, NewDatabaseError("seek_next_varint_byte", err, nil)
		}
	}

	return 0, 0, NewDatabaseError("varint_too_long", ErrInvalidVarint, nil)
}

// ReadVarintAt reads a variable-length integer from a specific offset
func (vr *VarintReader) ReadVarintAt(offset int64) (uint64, int, error) {
	if _, err := vr.reader.Seek(offset, 0); err != nil {
		return 0, 0, NewDatabaseError("seek_varint_offset", err, map[string]interface{}{
			"offset": offset,
		})
	}
	return vr.ReadVarint()
}
