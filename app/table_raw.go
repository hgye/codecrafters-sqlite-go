package main

import (
	"context"
	"fmt"
)

// TableRawImpl implements TableRaw interface for raw SQLite table operations
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

// ReadAllCells reads all cells from the table's root page using B-tree traversal
func (tr *TableRawImpl) ReadAllCells(ctx context.Context) ([]Cell, error) {
	// Use B-tree abstraction for traversal
	btree := NewBTree(tr.dbRaw, tr.rootPage, BTreeTypeTable)
	cells, err := btree.TraverseAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("traverse table %s B-tree: %w", tr.name, err)
	}

	return cells, nil
}

// GetRootPage returns the root page number
func (tr *TableRawImpl) GetRootPage() int {
	return tr.rootPage
}

// GetName returns the table name
func (tr *TableRawImpl) GetName() string {
	return tr.name
}

// ReadCellByRowid reads a specific cell by rowid using B-tree search
func (tr *TableRawImpl) ReadCellByRowid(ctx context.Context, targetRowid int64) (*Cell, error) {
	// Use B-tree search to find the specific rowid
	btree := NewBTree(tr.dbRaw, tr.rootPage, BTreeTypeTable)
	cells, err := btree.Search(ctx, uint64(targetRowid))
	if err != nil {
		return nil, fmt.Errorf("search for rowid %d: %w", targetRowid, err)
	}

	if len(cells) == 0 {
		return nil, fmt.Errorf("rowid %d not found in table %s", targetRowid, tr.name)
	}

	// Return first matching cell
	cell := cells[0]
	return &cell, nil
}
