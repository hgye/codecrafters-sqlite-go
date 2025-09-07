package main

import (
	"encoding/binary"
	"fmt"
)

// TableBTreeParser implements BTreeCellParser for table B-trees
type TableBTreeParser struct{}

// ParseLeafCell parses a leaf table cell
func (p *TableBTreeParser) ParseLeafCell(pageData []byte, offset int) (*BTreeCell, error) {
	if offset >= len(pageData) {
		return nil, fmt.Errorf("cell offset %d exceeds page size", offset)
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
	header, headerOffset := readRecordHeader(payload, 0)
	body, _, err := readRecordBody(payload, headerOffset, header)
	if err != nil {
		return nil, err
	}
	
	return &BTreeCell{
		Rowid: rowID,
		Record: Record{
			RecordHeader: header,
			RecordBody:   body,
		},
	}, nil
}

// ParseInteriorCell parses an interior table cell
func (p *TableBTreeParser) ParseInteriorCell(pageData []byte, offset int) (uint32, BTreeKey, error) {
	if offset+4 > len(pageData) {
		return 0, nil, fmt.Errorf("interior cell offset %d exceeds page size %d", offset, len(pageData))
	}
	
	// Interior table cell format: 4-byte child page number + varint rowid key
	childPageNum := binary.BigEndian.Uint32(pageData[offset : offset+4])
	offset += 4
	
	
	// Read rowid key
	rowid, _ := readVarint(pageData, offset)
	
	return childPageNum, uint64(rowid), nil
}

// ExtractSearchKey extracts the search key (rowid) from a table cell
func (p *TableBTreeParser) ExtractSearchKey(cell *BTreeCell) BTreeKey {
	return cell.Rowid
}

// MatchesSearchKey checks if a table cell matches the search rowid
func (p *TableBTreeParser) MatchesSearchKey(cell *BTreeCell, searchKey BTreeKey) bool {
	searchRowid, ok := searchKey.(uint64)
	if !ok {
		return false
	}
	return cell.Rowid == searchRowid
}

// IndexBTreeParser implements BTreeCellParser for index B-trees
type IndexBTreeParser struct{}

// ParseLeafCell parses a leaf index cell
func (p *IndexBTreeParser) ParseLeafCell(pageData []byte, offset int) (*BTreeCell, error) {
	if offset >= len(pageData) {
		return nil, fmt.Errorf("cell offset %d exceeds page size", offset)
	}
	
	// Leaf index cell format: varint payload_size, payload
	payloadSize, bytesRead := readVarint(pageData, offset)
	offset += bytesRead
	
	if offset+int(payloadSize) > len(pageData) {
		return nil, fmt.Errorf("payload extends beyond page boundary")
	}
	payload := pageData[offset : offset+int(payloadSize)]
	
	// Parse record from payload
	header, headerOffset := readRecordHeader(payload, 0)
	body, _, err := readRecordBody(payload, headerOffset, header)
	if err != nil {
		return nil, err
	}
	
	// For index cells, the last value in the record is typically the rowid
	var rowid uint64
	if len(body.Values) > 0 {
		// Try to extract rowid from the last value
		lastValue := body.Values[len(body.Values)-1]
		if bytes, ok := lastValue.([]byte); ok {
			rowid = BytesToInteger(bytes)
		}
	}
	
	return &BTreeCell{
		Rowid: rowid,
		Record: Record{
			RecordHeader: header,
			RecordBody:   body,
		},
	}, nil
}

// ParseInteriorCell parses an interior index cell
func (p *IndexBTreeParser) ParseInteriorCell(pageData []byte, offset int) (uint32, BTreeKey, error) {
	// fmt.Printf("DEBUG: ParseInteriorCell at offset 0x%x\n", offset)
	
	if offset+4 > len(pageData) {
		return 0, nil, fmt.Errorf("interior cell offset exceeds page size")
	}
	
	// Interior index cell format: 4-byte child page number, varint payload_size, payload (key)
	childPageNum := binary.BigEndian.Uint32(pageData[offset : offset+4])
	offset += 4
	
	// Read payload size
	payloadSize, bytesRead := readVarint(pageData, offset)
	// recordOffset := offset + bytesRead
	// fmt.Printf("DEBUG: Record starts at offset 0x%x, payload size: %d\n", recordOffset, payloadSize)
	offset += bytesRead
	
	if offset+int(payloadSize) > len(pageData) {
		return 0, nil, fmt.Errorf("payload extends beyond page boundary")
	}
	payload := pageData[offset : offset+int(payloadSize)]
	
	// Parse the key from payload
	header, headerOffset := readRecordHeader(payload, 0)
	body, _, err := readRecordBody(payload, headerOffset, header)
	if err != nil {
		return childPageNum, nil, err
	}
	
	// Extract the first key value for comparison
	var key interface{}
	if len(body.Values) > 0 {
		// Convert bytes to string for proper comparison
		if bytes, ok := body.Values[0].([]byte); ok {
			key = string(bytes)
		} else {
			key = body.Values[0]
		}
	}
	
	return childPageNum, key, nil
}

// ExtractSearchKey extracts the first indexed column value from an index cell  
func (p *IndexBTreeParser) ExtractSearchKey(cell *BTreeCell) BTreeKey {
	if len(cell.Record.RecordBody.Values) > 0 {
		value := cell.Record.RecordBody.Values[0]
		if bytes, ok := value.([]byte); ok {
			result := string(bytes)
			// Debug: show record structure for entries starting with 'er'
			// if len(result) >= 2 && result[:2] == "er" {
			// 	fmt.Printf("DEBUG: RECORD with 'er' - Values count: %d\n", len(cell.Record.RecordBody.Values))
			// 	for i, val := range cell.Record.RecordBody.Values {
			// 		if valBytes, ok := val.([]byte); ok {
			// 			fmt.Printf("DEBUG:   Value[%d]: '%s' (len=%d)\n", i, string(valBytes), len(valBytes))
			// 		} else {
			// 			fmt.Printf("DEBUG:   Value[%d]: %v (%T)\n", i, val, val)
			// 		}
			// 	}
			// }
			// if result == "eritrea" {
			// 	fmt.Printf("DEBUG: Found exact match 'eritrea'!\n")
			// }
			return result
		}
		return value
	}
	return nil
}

// MatchesSearchKey checks if an index cell matches the search key
func (p *IndexBTreeParser) MatchesSearchKey(cell *BTreeCell, searchKey BTreeKey) bool {
	cellKey := p.ExtractSearchKey(cell)
	if cellKey == nil {
		return false
	}
	
	// Convert both to strings for comparison
	var cellStr string
	if bytes, ok := cellKey.([]byte); ok {
		cellStr = string(bytes)
	} else {
		cellStr = fmt.Sprintf("%v", cellKey)
	}
	
	var searchStr string
	if bytes, ok := searchKey.([]byte); ok {
		searchStr = string(bytes)
	} else {
		searchStr = fmt.Sprintf("%v", searchKey)
	}
	
	// fmt.Printf("DEBUG: MatchesSearchKey - cellKey: %T '%s' vs searchKey: %T '%s' = %v\n", 
	// 	cellKey, cellStr, searchKey, searchStr, cellStr == searchStr)
	
	return cellStr == searchStr
}