package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

// DatabaseImpl implements DatabaseInterface
type DatabaseImpl struct {
	dbRaw        DatabaseRaw
	tables       map[string]Table // cached tables
	indexes      map[string]Index // cached indexes
	schemas      []SchemaRecord   // cached schema records
	schemaLoaded bool             // flag to track if schema is loaded
}

// NewDatabase creates a new logical database instance with functional options
func NewDatabase(filePath string, options ...DatabaseOption) (*DatabaseImpl, error) {
	dbRaw, err := NewDatabaseRaw(filePath, options...)
	if err != nil {
		return nil, err
	}

	db := &DatabaseImpl{
		dbRaw:        dbRaw,
		tables:       make(map[string]Table),
		indexes:      make(map[string]Index),
		schemas:      nil,
		schemaLoaded: false,
	}

	return db, nil
}

// LoadSchema loads and caches all schema records, tables, and indexes from the database
func (db *DatabaseImpl) LoadSchema(ctx context.Context) ([]SchemaRecord, error) {
	// Return cached schema if available
	if db.schemaLoaded {
		return db.schemas, nil
	}

	schemaCells, err := db.dbRaw.ReadSchemaTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("load schema: %w", err)
	}

	var schemas []SchemaRecord
	tables := make(map[string]Table)
	indexes := make(map[string]Index)

	for _, cell := range schemaCells {
		schema := cell.Record.RecordBody.ParseAsSchema()
		if schema != nil {
			schemas = append(schemas, *schema)
		}
	}

	// Single pass: create tables and indexes, and associate indexes with tables
	for _, schema := range schemas {
		if schema.Type == "table" && schema.Name != "sqlite_master" {
			tableRaw := NewTableRaw(db.dbRaw, schema.Name, int(schema.RootPage))
			// fmt.Fprintf(os.Stderr, "Creating table: %s\n", schema.Name)
			tableImpl := NewTable(tableRaw, &schema)
			tables[schema.Name] = Table(tableImpl)
		} else if schema.Type == "index" {
			indexRaw := NewIndexRaw(db.dbRaw, schema.Name, int(schema.RootPage), &schema)
			index := NewIndex(indexRaw, &schema)
			indexes[schema.Name] = index
			// Associate with table if it exists
			if table, ok := tables[schema.TblName]; ok {
				if tableImpl, ok := table.(*TableImpl); ok {
					tableImpl.AddIndex(index)
				}
			}
		}
	}

	db.schemas = schemas
	db.tables = tables
	db.indexes = indexes
	db.schemaLoaded = true

	return schemas, nil
}

// GetTables returns a list of all table names
func (db *DatabaseImpl) GetTables(ctx context.Context) ([]string, error) {
	if !db.schemaLoaded {
		_, err := db.LoadSchema(ctx)
		if err != nil {
			return nil, fmt.Errorf("get tables: %w", err)
		}
	}
	names := make([]string, 0, len(db.tables)+1)
	names = append(names, "sqlite_master")
	for name := range db.tables {
		// fmt.Fprintf(os.Stderr, "Found table: %s\n", name)
		if name != "sqlite_master" {
			names = append(names, name)
		}
	}
	return names, nil
}

// GetTable returns a table by name
func (db *DatabaseImpl) GetTable(ctx context.Context, name string) (Table, error) {
	if !db.schemaLoaded {
		_, err := db.LoadSchema(ctx)
		if err != nil {
			return nil, err
		}
	}
	if table, exists := db.tables[name]; exists {
		return table, nil
	}
	return nil, fmt.Errorf("table not found: %s", name)
}

// GetIndex returns an index by name
func (db *DatabaseImpl) GetIndex(ctx context.Context, name string) (Index, error) {
	if !db.schemaLoaded {
		_, err := db.LoadSchema(ctx)
		if err != nil {
			return nil, err
		}
	}
	if index, exists := db.indexes[name]; exists {
		return index, nil
	}
	return nil, fmt.Errorf("index not found: %s", name)
}

// GetIndices returns a list of all index names
func (db *DatabaseImpl) GetIndices(ctx context.Context) ([]string, error) {
	if !db.schemaLoaded {
		_, err := db.LoadSchema(ctx)
		if err != nil {
			return nil, fmt.Errorf("get indices: %w", err)
		}
	}
	names := make([]string, 0, len(db.indexes))
	for name := range db.indexes {
		names = append(names, name)
	}
	return names, nil
}

// Close closes the database
func (db *DatabaseImpl) Close() error {
	return db.dbRaw.Close()
}

// GetPageSize returns the database page size
func (db *DatabaseImpl) GetPageSize() int {
	return db.dbRaw.GetPageSize()
}

// ClearCache clears all cached data (tables and schema)
func (db *DatabaseImpl) ClearCache() {
	db.tables = make(map[string]Table)
	db.indexes = make(map[string]Index)
	db.schemas = nil
	db.schemaLoaded = false
}

// parseTableSchema parses table schema from CREATE TABLE SQL
func parseTableSchema(schemaSQL string) ([]Column, error) {
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

		// Parse columns and detect PRIMARY KEY and AUTOINCREMENT
		columns := make([]Column, len(parsedStmt.TableSpec.Columns))

		for i, col := range parsedStmt.TableSpec.Columns {
			isAutoIncrement := bool(col.Type.Autoincrement)
			isIntegerPrimaryKey := isAutoIncrement && strings.ToUpper(col.Type.Type) == "INTEGER"

			columns[i] = Column{
				Name:            col.Name.String(),
				Type:            col.Type.Type,
				Index:           i,
				Nullable:        true,                // Default assumption
				IsPrimaryKey:    isIntegerPrimaryKey, // In SQLite, INTEGER PRIMARY KEY AUTOINCREMENT is the primary key
				IsAutoIncrement: isAutoIncrement,
			}

			// Primary key columns are not nullable
			// if isIntegerPrimaryKey {
			// 	columns[i].Nullable = false
			// }
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
	// Fix SQLite quoted identifiers - replace double quotes with nothing
	// SQLite uses double quotes, MySQL sqlparser doesn't like them for table names
	normalized := strings.ReplaceAll(sql, `"`, "")

	// Fix MySQL syntax: "primary key autoincrement" should be "AUTO_INCREMENT PRIMARY KEY"
	normalized = strings.ReplaceAll(normalized, "primary key autoincrement", "AUTO_INCREMENT PRIMARY KEY")
	normalized = strings.ReplaceAll(normalized, "PRIMARY KEY AUTOINCREMENT", "AUTO_INCREMENT PRIMARY KEY")

	// Handle column names with spaces - wrap them in backticks for MySQL compatibility
	normalized = handleColumnNamesWithSpaces(normalized)

	// Trim leading/trailing whitespace
	normalized = strings.TrimSpace(normalized)

	return normalized
}

// handleColumnNamesWithSpaces wraps column names that contain spaces in backticks
func handleColumnNamesWithSpaces(sql string) string {
	// Replace "size range" specifically (case insensitive)
	sql = strings.ReplaceAll(sql, "size range", "`size range`")
	sql = strings.ReplaceAll(sql, "SIZE RANGE", "`SIZE RANGE`")

	return sql
}

// loadTableIndexes loads all indexes associated with a table and adds them to the table
func (db *DatabaseImpl) loadTableIndexes(ctx context.Context, table *TableImpl, schemas []SchemaRecord) error {
	tableName := table.GetName()

	// Find all indexes that belong to this table
	for _, schema := range schemas {
		if schema.Type == "index" && schema.TblName == tableName {
			// Create index
			index, err := db.createIndexFromSchema(ctx, &schema)
			if err != nil {
				return fmt.Errorf("create index %s for table %s: %w", schema.Name, tableName, err)
			}

			// Add index to table
			table.AddIndex(index)
		}
	}

	return nil
}

// createIndexFromSchema creates an index from a schema record
func (db *DatabaseImpl) createIndexFromSchema(ctx context.Context, schema *SchemaRecord) (Index, error) {
	// Create raw index
	indexRaw := NewIndexRaw(db.dbRaw, schema.Name, int(schema.RootPage), schema)

	// Create logical index
	index := NewIndex(indexRaw, schema)

	return index, nil
}

// GetTableIndexes returns all indexes for a specific table
func (db *DatabaseImpl) GetTableIndexes(ctx context.Context, tableName string) ([]Index, error) {
	table, err := db.GetTable(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("get table %s for indexes: %w", tableName, err)
	}

	// Type assert to get the concrete implementation
	tableImpl, ok := table.(*TableImpl)
	if !ok {
		return nil, fmt.Errorf("table %s is not a TableImpl", tableName)
	}

	return tableImpl.GetIndexes(ctx)
}
