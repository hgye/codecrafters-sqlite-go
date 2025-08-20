package main

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

// DatabaseService provides high-level database operations
type DatabaseService struct {
	db           *SQLiteDB
	pageReader   *PageReader
	varintReader *VarintReader
}

// NewDatabaseService creates a new database service
func NewDatabaseService(db *SQLiteDB) *DatabaseService {
	pageReader := NewPageReader(db, db.header.PageSize)
	varintReader := NewVarintReader(db)

	return &DatabaseService{
		db:           db,
		pageReader:   pageReader,
		varintReader: varintReader,
	}
}

// GetTableSchema returns the schema for a table
func (ds *DatabaseService) GetTableSchema(tableName string) ([]*Column, error) {
	table := ds.db.GetTable(tableName)
	if table == nil {
		return nil, NewDatabaseError("get_table_schema", ErrTableNotFound, map[string]interface{}{
			"table_name": tableName,
		})
	}

	return ds.parseTableSchema(table.SchemaSQL)
}

// GetTableRows returns all rows from a table
func (ds *DatabaseService) GetTableRows(tableName string) ([]*Row, error) {
	table := ds.db.GetTable(tableName)
	if table == nil {
		return nil, NewDatabaseError("get_table_rows", ErrTableNotFound, map[string]interface{}{
			"table_name": tableName,
		})
	}

	cells, err := ds.readTableCells(table)
	if err != nil {
		return nil, err
	}

	rows := make([]*Row, len(cells))
	for i, cell := range cells {
		row, err := ds.cellToRow(cell)
		if err != nil {
			return nil, NewDatabaseError("convert_cell_to_row", err, map[string]interface{}{
				"row_index": i,
			})
		}
		rows[i] = row
	}

	return rows, nil
}

// GetColumnValues returns all values for a specific column
func (ds *DatabaseService) GetColumnValues(tableName string, columnName string) ([]Value, error) {
	schema, err := ds.GetTableSchema(tableName)
	if err != nil {
		return nil, err
	}

	columnIndex := -1
	for _, col := range schema {
		if strings.EqualFold(col.Name, columnName) {
			columnIndex = col.Index
			break
		}
	}

	if columnIndex == -1 {
		return nil, NewDatabaseError("find_column", ErrColumnNotFound, map[string]interface{}{
			"column_name": columnName,
			"table_name":  tableName,
		})
	}

	rows, err := ds.GetTableRows(tableName)
	if err != nil {
		return nil, err
	}

	values := make([]Value, len(rows))
	for i, row := range rows {
		value, err := row.Get(columnIndex)
		if err != nil {
			return nil, NewDatabaseError("get_column_value_from_row", err, map[string]interface{}{
				"row_index":    i,
				"column_index": columnIndex,
			})
		}
		values[i] = value
	}

	return values, nil
}

// GetRowCount returns the number of rows in a table
func (ds *DatabaseService) GetRowCount(tableName string) (int, error) {
	table := ds.db.GetTable(tableName)
	if table == nil {
		return 0, NewDatabaseError("get_row_count", ErrTableNotFound, map[string]interface{}{
			"table_name": tableName,
		})
	}

	// For efficiency, we can get the count from the page header
	pageHeader, err := ds.pageReader.ReadPageHeader(uint32(table.RootPage))
	if err != nil {
		return 0, err
	}

	return int(pageHeader.CellCount), nil
}

// readTableCells reads all cells from a table
func (ds *DatabaseService) readTableCells(table *Table) ([]*Cell, error) {
	pageHeader, err := ds.pageReader.ReadPageHeader(uint32(table.RootPage))
	if err != nil {
		return nil, err
	}

	if pageHeader.PageType != 0x0d {
		return nil, NewDatabaseError("validate_page_type", ErrInvalidPageType, map[string]interface{}{
			"expected_type": 0x0d,
			"actual_type":   pageHeader.PageType,
		})
	}

	cellPointers, err := ds.pageReader.ReadCellPointers(uint32(table.RootPage), pageHeader.CellCount)
	if err != nil {
		return nil, err
	}

	cells := make([]*Cell, len(cellPointers))
	for i, pointer := range cellPointers {
		cell, err := ds.readTableCell(uint32(table.RootPage), pointer)
		if err != nil {
			return nil, NewDatabaseError("read_table_cell", err, map[string]interface{}{
				"cell_index":   i,
				"cell_pointer": pointer,
				"table_name":   table.Name,
			})
		}
		cells[i] = cell
	}

	return cells, nil
}

// readTableCell reads a single table cell
func (ds *DatabaseService) readTableCell(pageNum uint32, pointer CellPointer) (*Cell, error) {
	// Calculate absolute offset
	pageOffset := ds.pageReader.calculatePageOffset(pageNum)
	cellOffset := pageOffset + int64(pointer.Offset())

	// Read payload size and rowid varints
	payloadSize, payloadSizeBytes, err := ds.varintReader.ReadVarintAt(cellOffset)
	if err != nil {
		return nil, err
	}

	rowid, rowidBytes, err := ds.varintReader.ReadVarintAt(cellOffset + int64(payloadSizeBytes))
	if err != nil {
		return nil, err
	}

	// Calculate payload start
	payloadStart := cellOffset + int64(payloadSizeBytes) + int64(rowidBytes)

	// Read payload data
	payload := make([]byte, payloadSize)
	if _, err := ds.db.ReadAt(payload, payloadStart); err != nil {
		return nil, NewDatabaseError("read_payload", err, map[string]interface{}{
			"payload_start": payloadStart,
			"payload_size":  payloadSize,
		})
	}

	// Parse record
	record, err := ds.parseRecord(payload)
	if err != nil {
		return nil, err
	}

	return &Cell{
		PayloadSize: payloadSize,
		Rowid:       rowid,
		Record:      *record,
	}, nil
}

// parseRecord parses a record from payload data
func (ds *DatabaseService) parseRecord(payload []byte) (*Record, error) {
	// Parse header
	headerSize, headerSizeBytes := readVarint(payload, 0)
	offset := headerSizeBytes

	var serialTypes []uint8
	for offset < int(headerSize) {
		serialType, bytes := readVarint(payload, offset)
		serialTypes = append(serialTypes, uint8(serialType))
		offset += bytes
	}

	// Parse body
	values := make([]Value, len(serialTypes))
	for i, serialType := range serialTypes {
		size := getSerialTypeSize(serialType)
		if size == 0 {
			values[i] = NewSQLiteValue(serialType, nil)
			continue
		}

		if offset+size > len(payload) {
			return nil, NewDatabaseError("parse_record_value", ErrInsufficientData, map[string]interface{}{
				"value_index":  i,
				"needed_size":  offset + size,
				"payload_size": len(payload),
			})
		}

		data := payload[offset : offset+size]
		values[i] = NewSQLiteValue(serialType, data)
		offset += size
	}

	return &Record{
		RecordHeader: RecordHeader{
			HeaderSize:  headerSize,
			SerialTypes: serialTypes,
		},
		RecordBody: RecordBody{
			Values: convertValuesToInterface(values),
		},
	}, nil
}

// cellToRow converts a cell to a row
func (ds *DatabaseService) cellToRow(cell *Cell) (*Row, error) {
	values := make([]Value, len(cell.Record.RecordBody.Values))
	for i, val := range cell.Record.RecordBody.Values {
		if val == nil {
			values[i] = NewSQLiteValue(0, nil) // NULL value
		} else if bytes, ok := val.([]byte); ok {
			// Determine serial type from the header
			if i < len(cell.Record.RecordHeader.SerialTypes) {
				serialType := cell.Record.RecordHeader.SerialTypes[i]
				values[i] = NewSQLiteValue(serialType, bytes)
			} else {
				values[i] = NewSQLiteValue(12, bytes) // Default to BLOB
			}
		} else {
			// Handle other value types
			values[i] = NewSQLiteValue(12, []byte(fmt.Sprintf("%v", val)))
		}
	}

	return &Row{Values: values}, nil
}

// parseTableSchema parses table schema from CREATE TABLE SQL
func (ds *DatabaseService) parseTableSchema(schemaSQL string) ([]*Column, error) {
	// For now, let's use the existing sqlparser approach that was working
	// Normalize SQLite syntax to MySQL syntax for sqlparser
	normalizedSQL := normalizeSQLiteToMySQL(schemaSQL)

	// Try to parse with sqlparser
	stmt, err := sqlparser.Parse(normalizedSQL)
	if err != nil {
		return nil, NewDatabaseError("parse_schema_sql", err, map[string]interface{}{
			"schema_sql":     schemaSQL,
			"normalized_sql": normalizedSQL,
		})
	}

	switch parsedStmt := stmt.(type) {
	case *sqlparser.DDL:
		if parsedStmt.Action != "create" || parsedStmt.TableSpec == nil {
			return nil, NewDatabaseError("invalid_ddl_statement", ErrInvalidDatabase, map[string]interface{}{
				"action": parsedStmt.Action,
			})
		}

		columns := make([]*Column, len(parsedStmt.TableSpec.Columns))
		for i, col := range parsedStmt.TableSpec.Columns {
			columns[i] = &Column{
				Name:     col.Name.String(),
				Type:     col.Type.Type,
				Index:    i,
				Nullable: true, // Default assumption
			}
		}

		return columns, nil

	default:
		return nil, NewDatabaseError("unsupported_schema_statement", ErrInvalidDatabase, map[string]interface{}{
			"statement_type": parsedStmt,
		})
	}
}

// normalizeSQLiteToMySQL converts SQLite-specific syntax to MySQL syntax for sqlparser
func normalizeSQLiteToMySQL(sql string) string {
	// Fix MySQL syntax: "primary key autoincrement" should be "AUTO_INCREMENT PRIMARY KEY"
	// or just "AUTO_INCREMENT" (as AUTO_INCREMENT implies PRIMARY KEY in MySQL)
	normalized := strings.ReplaceAll(sql, "primary key autoincrement", "AUTO_INCREMENT PRIMARY KEY")
	normalized = strings.ReplaceAll(normalized, "PRIMARY KEY AUTOINCREMENT", "AUTO_INCREMENT PRIMARY KEY")
	return normalized
}

// convertValuesToInterface converts Value slice to interface{} slice for compatibility
func convertValuesToInterface(values []Value) []interface{} {
	result := make([]interface{}, len(values))
	for i, val := range values {
		if val == nil {
			result[i] = nil
		} else {
			result[i] = val.Raw()
		}
	}
	return result
}
