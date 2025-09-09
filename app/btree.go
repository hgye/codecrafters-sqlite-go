package main

import (
	"context"
	"encoding/binary"
	"fmt"
)

// BTreeType represents the type of B-tree (table or index)
type BTreeType int

const (
	BTreeTypeTable BTreeType = iota
	BTreeTypeIndex
)

// BTreeKey represents a key in the B-tree
type BTreeKey interface{}

// BTreeComparator defines how to compare keys in the B-tree
type BTreeComparator func(key1, key2 BTreeKey) int

// BTreeCellParser defines how to parse cells for different B-tree types
type BTreeCellParser interface {
	// ParseLeafCell parses a leaf cell and returns the key and value
	ParseLeafCell(pageData []byte, offset int) (*Cell, error)

	// ParseInteriorCell parses an interior cell and returns child page and key
	ParseInteriorCell(pageData []byte, offset int) (childPage uint32, key BTreeKey, err error)

	// ExtractSearchKey extracts the key used for searching from a cell
	ExtractSearchKey(cell *Cell) BTreeKey

	// MatchesSearchKey checks if a cell matches the search criteria
	MatchesSearchKey(cell *Cell, searchKey BTreeKey) bool
}

// BTree provides generic B-tree traversal functionality
type BTree struct {
	dbRaw      DatabaseRaw
	rootPage   int
	btreeType  BTreeType
	parser     BTreeCellParser
	comparator BTreeComparator
}

// NewBTree creates a new B-tree instance
func NewBTree(dbRaw DatabaseRaw, rootPage int, btreeType BTreeType) *BTree {
	bt := &BTree{
		dbRaw:     dbRaw,
		rootPage:  rootPage,
		btreeType: btreeType,
	}

	// Set up parser and comparator based on type
	switch btreeType {
	case BTreeTypeTable:
		bt.parser = &TableBTreeParser{}
		bt.comparator = compareRowids
	case BTreeTypeIndex:
		bt.parser = &IndexBTreeParser{}
		bt.comparator = compareIndexKeys
	}

	return bt
}

// TraverseAll traverses the entire B-tree and returns all cells
func (bt *BTree) TraverseAll(ctx context.Context) ([]Cell, error) {
	return bt.traversePage(ctx, bt.rootPage)
}

// Search performs a B-tree search for the given key
func (bt *BTree) Search(ctx context.Context, searchKey BTreeKey) ([]Cell, error) {
	// Use proper B-tree navigation for both table and index B-trees
	// No fallback to TraverseAll() - for manual debugging
	// fmt.Printf("DEBUG: Starting search from root page %d (0x%x)\n", bt.rootPage, bt.rootPage)
	return bt.searchPage(ctx, bt.rootPage, searchKey)
}

// SearchRange performs a range search in the B-tree
func (bt *BTree) SearchRange(ctx context.Context, startKey, endKey BTreeKey) ([]Cell, error) {
	// For now, just traverse all and filter
	// This could be optimized to use B-tree properties
	allCells, err := bt.TraverseAll(ctx)
	if err != nil {
		return nil, err
	}

	var results []Cell
	for _, cell := range allCells {
		key := bt.parser.ExtractSearchKey(&cell)
		if bt.comparator(key, startKey) >= 0 && bt.comparator(key, endKey) <= 0 {
			results = append(results, cell)
		}
	}
	return results, nil
}

// traversePage recursively traverses a B-tree page
func (bt *BTree) traversePage(ctx context.Context, pageNum int) ([]Cell, error) {

	headerOffset := 0

	if pageNum == 1 {
		headerOffset = 100
	}
	pageData, err := bt.dbRaw.ReadPage(ctx, pageNum)
	if err != nil {
		return nil, fmt.Errorf("read page %d: %w", pageNum, err)
	}

	pageHeader, err := bt.parsePageHeaderAtOffset(pageData, headerOffset)
	if err != nil {
		return nil, fmt.Errorf("parse page header: %w", err)
	}

	if bt.isLeafPage(pageHeader) {
		return bt.readLeafCells(ctx, pageHeader, pageData, pageNum)
	}

	return bt.traverseInteriorPage(ctx, pageHeader, pageData)
}

// searchPage performs B-tree search on a page
func (bt *BTree) searchPage(ctx context.Context, pageNum int, searchKey BTreeKey) ([]Cell, error) {
	// fmt.Printf("DEBUG: Searching page %d for key '%v'\n", pageNum, searchKey)
	pageData, err := bt.dbRaw.ReadPage(ctx, pageNum)
	if err != nil {
		return nil, fmt.Errorf("read page %d: %w", pageNum, err)
	}

	headerOffset := 0
	// Special handling for page 1 (sqlite_master table with 100-byte database header)
	if pageNum == 1 {
		headerOffset = 100
	}
	// Page 1 has a 100-byte database header, so page header starts at offset 100
	// 	const headerOffset = 100

	// 	if len(pageData) < headerOffset+8 {
	// 		return nil, fmt.Errorf("page 1 too small: have %d bytes, need at least %d",
	// 			len(pageData), headerOffset+8)
	// 	}

	// 	// Parse page header at the correct offset for page 1
	// 	pageHeader, err := bt.parsePageHeaderAtOffset(pageData, headerOffset)
	// 	if err != nil {
	// 		return nil, fmt.Errorf("parse page 1 header: %w", err)
	// 	}

	// 	// Page 1 should always be a leaf page (sqlite_master table)
	// 	if !bt.isLeafPage(pageHeader) {
	// 		return nil, fmt.Errorf("page 1 should be a leaf page, got type 0x%02X", pageHeader.PageType)
	// 	}

	// 	// Search leaf cells - use regular method since pageHeader already has correct offset
	// 	return bt.searchLeafPage(ctx, pageHeader, pageData, searchKey, 1)
	// }

	// Show first 16 bytes of page for debugging
	// fmt.Printf("DEBUG: Page %d first 16 bytes: %x\n", pageNum, pageData[:16])

	pageHeader, err := bt.parsePageHeaderAtOffset(pageData, headerOffset)
	if err != nil {
		return nil, fmt.Errorf("parse page header: %w", err)
	}

	// fmt.Printf("DEBUG: Page %d (0x%x) has type 0x%02x, isLeaf=%v\n", pageNum, pageNum, pageHeader.PageType, bt.isLeafPage(pageHeader))

	if bt.isLeafPage(pageHeader) {
		// fmt.Printf("DEBUG: Page %d is LEAF page with %d cells\n", pageNum, pageHeader.CellCount)
		return bt.searchLeafPage(ctx, pageHeader, pageData, searchKey, pageNum)
	}

	// Interior page - find the right child
	// fmt.Printf("DEBUG: Page %d is INTERIOR page with %d cells\n", pageNum, pageHeader.CellCount)
	childPage := bt.findChildForKey(pageNum, pageHeader, pageData, searchKey)
	// fmt.Printf("DEBUG: Selected child page %d for key '%v'\n", childPage, searchKey)
	return bt.searchPage(ctx, childPage, searchKey)
}

// isLeafPage checks if a page is a leaf page
func (bt *BTree) isLeafPage(header *PageHeader) bool {
	switch bt.btreeType {
	case BTreeTypeTable:
		return header.IsLeafTable()
	case BTreeTypeIndex:
		return header.IsLeafIndex()
	default:
		return false
	}
}

// readLeafCells reads all cells from a leaf page
func (bt *BTree) readLeafCells(ctx context.Context, header *PageHeader, pageData []byte, pageNum int) ([]Cell, error) {
	var cells []Cell

	// Calculate cell pointer offset
	cellPointerOffset := bt.getCellPointerOffset(header)

	// For page 1, we need to account for the 100-byte database header
	// The page header was parsed at offset 100, so cell pointers start at offset 108
	if pageNum == 1 {
		// This is page 1 with database header
		cellPointerOffset = 108 // 100 (db header) + 8 (page header)
	}

	errorHandler := NewErrorHandler(ErrorStrategySkip, nil)

	for i := uint16(0); i < header.CellCount; i++ {
		offset := cellPointerOffset + int(i*2)
		if offset+1 >= len(pageData) {
			break
		}
		cellOffset := int(binary.BigEndian.Uint16(pageData[offset : offset+2]))

		cell, err := bt.parser.ParseLeafCell(pageData, cellOffset)
		if handledErr := errorHandler.HandleProcessingError(err, fmt.Sprintf("parse leaf cell %d", i)); handledErr != nil {
			return nil, handledErr
		}
		if err == nil {
			cells = append(cells, *cell)
		}
	}

	return cells, nil
} // searchLeafPage searches for matching cells in a leaf page
func (bt *BTree) searchLeafPage(ctx context.Context, header *PageHeader, pageData []byte, searchKey BTreeKey, pageNum int) ([]Cell, error) {
	var results []Cell

	// Calculate cell pointer offset
	cellPointerOffset := bt.getCellPointerOffset(header)

	// For page 1, we need to account for the 100-byte database header
	// The page header was parsed at offset 100, so cell pointers start at offset 108
	if pageNum == 1 {
		// This is page 1 with database header
		cellPointerOffset = 108 // 100 (db header) + 8 (page header)
	}

	errorHandler := NewErrorHandler(ErrorStrategySkip, nil)

	for i := uint16(0); i < header.CellCount; i++ {
		offset := cellPointerOffset + int(i*2)
		if offset+1 >= len(pageData) {
			break
		}
		cellOffset := int(binary.BigEndian.Uint16(pageData[offset : offset+2]))

		cell, err := bt.parser.ParseLeafCell(pageData, cellOffset)
		if handledErr := errorHandler.HandleProcessingError(err, fmt.Sprintf("parse search cell %d", i)); handledErr != nil {
			return nil, handledErr
		}
		if err == nil && bt.parser.MatchesSearchKey(cell, searchKey) {
			results = append(results, *cell)
		}
	}

	return results, nil
}

// traverseInteriorPage traverses all children of an interior page
func (bt *BTree) traverseInteriorPage(ctx context.Context, header *PageHeader, pageData []byte) ([]Cell, error) {
	var allCells []Cell

	// Read rightmost child pointer
	rightmostChild := bt.getRightmostChild(pageData)
	cellPointerOffset := bt.getCellPointerOffset(header)

	// Process all child pages referenced by cells
	for i := uint16(0); i < header.CellCount; i++ {
		offset := cellPointerOffset + int(i*2)
		if offset+1 >= len(pageData) {
			break
		}
		cellOffset := int(binary.BigEndian.Uint16(pageData[offset : offset+2]))

		childPage, _, err := bt.parser.ParseInteriorCell(pageData, cellOffset)
		if err != nil {
			continue
		}

		childCells, err := bt.traversePage(ctx, int(childPage))
		if err != nil {
			continue
		}
		allCells = append(allCells, childCells...)
	}

	// Process rightmost child
	rightCells, err := bt.traversePage(ctx, int(rightmostChild))
	if err == nil {
		allCells = append(allCells, rightCells...)
	}

	return allCells, nil
}

// findChildForKey finds the appropriate child page for a search key
func (bt *BTree) findChildForKey(pageNum int, header *PageHeader, pageData []byte, searchKey BTreeKey) int {
	rightmostChild := bt.getRightmostChild(pageData)
	cellPointerOffset := bt.getCellPointerOffset(header)

	for i := uint16(0); i < header.CellCount; i++ {
		offset := cellPointerOffset + int(i*2)
		if offset+1 >= len(pageData) {
			break
		}
		cellOffset := int(binary.BigEndian.Uint16(pageData[offset : offset+2]))
		// fmt.Printf("DEBUG: Page %d (0x%x) parsing cell at offset 0x%x\n", pageNum, pageNum, cellOffset)
		childPage, cellKey, err := bt.parser.ParseInteriorCell(pageData, cellOffset)
		if err != nil {
			continue
		}

		if bt.comparator(searchKey, cellKey) <= 0 {
			return int(childPage)
		}
	}

	return int(rightmostChild)
}

// getCellPointerOffset returns the offset where cell pointers start
func (bt *BTree) getCellPointerOffset(header *PageHeader) int {
	if header.IsLeafTable() || header.IsLeafIndex() {
		return 8 // After page header
	}
	// Interior pages have rightmost pointer
	return 12 // After page header and rightmost pointer
}

// getRightmostChild reads the rightmost child pointer for interior pages
func (bt *BTree) getRightmostChild(pageData []byte) uint32 {
	if len(pageData) < 12 {
		return 0
	}
	return binary.BigEndian.Uint32(pageData[8:12])
}

// parsePageHeaderAtOffset parses a page header at a specific offset (for page 1)
func (bt *BTree) parsePageHeaderAtOffset(pageData []byte, offset int) (*PageHeader, error) {
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

// Comparator functions

func compareRowids(key1, key2 BTreeKey) int {
	rowid1, ok1 := key1.(uint64)
	rowid2, ok2 := key2.(uint64)
	if !ok1 || !ok2 {
		return 0
	}

	if rowid1 < rowid2 {
		return -1
	} else if rowid1 > rowid2 {
		return 1
	}
	return 0
}

func compareIndexKeys(key1, key2 BTreeKey) int {
	// Convert to proper strings for comparison
	var str1 string
	if bytes, ok := key1.([]byte); ok {
		str1 = string(bytes)
	} else {
		str1 = fmt.Sprintf("%v", key1)
	}

	var str2 string
	if bytes, ok := key2.([]byte); ok {
		str2 = string(bytes)
	} else {
		str2 = fmt.Sprintf("%v", key2)
	}

	// fmt.Printf("DEBUG: Comparing index keys: %T '%s' vs %T '%s'\n", key1, str1, key2, str2)

	if str1 < str2 {
		return -1
	} else if str1 > str2 {
		return 1
	}
	return 0
}
