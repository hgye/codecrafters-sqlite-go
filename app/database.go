package main

import (
	"strings"

	"github.com/xwb1989/sqlparser"
)

// DatabaseImpl implements DatabaseInterface
type DatabaseImpl struct {
	dbRaw        DatabaseRaw
	tables       map[string]Table // cached tables
	schemas      []SchemaRecord   // cached schema records
	schemaLoaded bool             // flag to track if schema is loaded
}

// NewDatabase creates a new logical database instance
func NewDatabase(filePath string) (*DatabaseImpl, error) {
	dbRaw, err := NewDatabaseRaw(filePath)
	if err != nil {
		return nil, err
	}

	db := &DatabaseImpl{
		dbRaw:        dbRaw,
		tables:       make(map[string]Table),
		schemas:      nil,
		schemaLoaded: false,
	}

	return db, nil
}

// GetSchema returns all schema records from the database
func (db *DatabaseImpl) GetSchema() ([]SchemaRecord, error) {
	// Return cached schema if available
	if db.schemaLoaded {
		return db.schemas, nil
	}

	schemaCells, err := db.dbRaw.ReadSchemaTable()
	if err != nil {
		return nil, NewDatabaseError("get_schema", err, nil)
	}

	var schemas []SchemaRecord
	for _, cell := range schemaCells {
		schema := cell.Record.RecordBody.ParseAsSchema()
		if schema != nil {
			schemas = append(schemas, *schema)
		}
	}

	// Cache the schema
	db.schemas = schemas
	db.schemaLoaded = true

	return schemas, nil
}

// GetTable returns a table by name
func (db *DatabaseImpl) GetTable(name string) (Table, error) {
	// Check cache first
	if table, exists := db.tables[name]; exists {
		return table, nil
	}

	// Get schema records (this will use cache if available)
	schemas, err := db.GetSchema()
	if err != nil {
		return nil, NewDatabaseError("read_schema", err, map[string]interface{}{
			"table_name": name,
		})
	}

	// Find table in schema
	for _, schema := range schemas {
		if schema.Type == "table" && schema.Name == name {
			// Create raw table
			tableRaw := NewTableRaw(db.dbRaw, schema.Name, int(schema.RootPage))

			// Create logical table
			table := NewTable(tableRaw, &schema)

			// Cache and return
			db.tables[name] = table
			return table, nil
		}
	}

	return nil, NewDatabaseError("get_table", ErrTableNotFound, map[string]interface{}{
		"table_name": name,
	})
}

// GetTables returns a list of all table names
func (db *DatabaseImpl) GetTables() ([]string, error) {
	// Get schema records (this will use cache if available)
	schemas, err := db.GetSchema()
	if err != nil {
		return nil, NewDatabaseError("get_tables", err, nil)
	}

	var tables []string
	tables = append(tables, "sqlite_master") // First table is always the schema table

	for _, schema := range schemas {
		if schema.Type == "table" && schema.Name != "sqlite_master" {
			tables = append(tables, schema.Name)
		}
	}

	return tables, nil
}

// Close closes the database
func (db *DatabaseImpl) Close() error {
	return db.dbRaw.Close()
}

// ClearCache clears all cached data (tables and schema)
func (db *DatabaseImpl) ClearCache() {
	db.tables = make(map[string]Table)
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

		columns := make([]Column, len(parsedStmt.TableSpec.Columns))
		for i, col := range parsedStmt.TableSpec.Columns {
			columns[i] = Column{
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
	normalized := strings.ReplaceAll(sql, "primary key autoincrement", "AUTO_INCREMENT PRIMARY KEY")
	normalized = strings.ReplaceAll(normalized, "PRIMARY KEY AUTOINCREMENT", "AUTO_INCREMENT PRIMARY KEY")
	return normalized
}
