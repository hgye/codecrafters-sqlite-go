package main

import (
	"os"
	"reflect"
	"testing"
)

func TestCellPointerMethods(t *testing.T) {
	cp := CellPointer(1024)

	if cp.Offset() != 1024 {
		t.Errorf("CellPointer.Offset() = %v, want %v", cp.Offset(), 1024)
	}

	if !cp.IsValid() {
		t.Errorf("CellPointer.IsValid() = %v, want %v", cp.IsValid(), true)
	}

	cpZero := CellPointer(0)
	if cpZero.IsValid() {
		t.Errorf("CellPointer(0).IsValid() = %v, want %v", cpZero.IsValid(), false)
	}
}

func TestNewSQLiteDB_FileNotFound(t *testing.T) {
	_, err := NewSQLiteDB("/nonexistent/path/to/database.db")
	if err == nil {
		t.Errorf("NewSQLiteDB() with nonexistent file should return error")
	}
}

func TestSQLiteDB_WithRealDatabase(t *testing.T) {
	// Skip if sample.db doesn't exist
	dbPath := "../sample.db"
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Skip("sample.db not found, skipping integration test")
	}

	db, err := NewSQLiteDB(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteDB() error = %v", err)
	}
	defer db.Close()

	// Test basic functionality
	if db.GetPageSize() == 0 {
		t.Errorf("GetPageSize() should return non-zero value")
	}

	tableNames := db.GetTableNames()
	if len(tableNames) == 0 {
		t.Errorf("GetTableNames() should return at least sqlite_master")
	}

	// Should always include sqlite_master
	found := false
	for _, name := range tableNames {
		if name == "sqlite_master" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("GetTableNames() should include sqlite_master, got: %v", tableNames)
	}

	tables := db.GetTables()
	if len(tables) == 0 {
		t.Errorf("GetTables() should return some tables")
	}

	schema := db.GetSchema()
	if len(schema) == 0 {
		t.Errorf("GetSchema() should return schema cells")
	}

	schemaObjects := db.GetSchemaObjects()
	if len(schemaObjects) == 0 {
		t.Errorf("GetSchemaObjects() should return schema objects")
	}

	// Test that all schema objects are properly parsed
	for _, obj := range schemaObjects {
		if obj.Type == "" {
			t.Errorf("Schema object should have a type")
		}
		if obj.Name == "" {
			t.Errorf("Schema object should have a name")
		}
	}
}

// Mock database for testing without file I/O
type MockSQLiteDB struct {
	pageSize uint16
	schema   []*Cell
}

func (m *MockSQLiteDB) GetPageSize() uint16 {
	return m.pageSize
}

func (m *MockSQLiteDB) GetSchema() []*Cell {
	return m.schema
}

func createMockSchemaCell(objType, name, tblName, sql string, rootPage uint8) *Cell {
	return &Cell{
		Record: Record{
			RecordBody: RecordBody{
				Values: []interface{}{
					[]byte(objType),
					[]byte(name),
					[]byte(tblName),
					[]byte{rootPage},
					[]byte(sql),
				},
			},
		},
	}
}

func TestGetTableNamesLogic(t *testing.T) {
	// Create mock database with known schema
	mockDB := &SQLiteDB{
		schemaTable: []*Cell{
			createMockSchemaCell("table", "users", "users", "CREATE TABLE users(id INTEGER, name TEXT)", 2),
			createMockSchemaCell("table", "posts", "posts", "CREATE TABLE posts(id INTEGER, title TEXT)", 3),
			createMockSchemaCell("index", "idx_users_name", "users", "CREATE INDEX idx_users_name ON users(name)", 4),
			createMockSchemaCell("view", "user_posts", "user_posts", "CREATE VIEW user_posts AS SELECT * FROM users JOIN posts", 0),
		},
	}

	tableNames := mockDB.GetTableNames()
	expected := []string{"sqlite_master", "users", "posts"}

	if !reflect.DeepEqual(tableNames, expected) {
		t.Errorf("GetTableNames() = %v, want %v", tableNames, expected)
	}
}

func TestGetTablesLogic(t *testing.T) {
	// Create mock database with known schema
	mockDB := &SQLiteDB{
		schemaTable: []*Cell{
			createMockSchemaCell("table", "users", "users", "CREATE TABLE users(id INTEGER, name TEXT)", 2),
			createMockSchemaCell("index", "idx_users_name", "users", "CREATE INDEX idx_users_name ON users(name)", 4),
			createMockSchemaCell("table", "posts", "posts", "CREATE TABLE posts(id INTEGER, title TEXT)", 3),
		},
	}

	tables := mockDB.GetTables()

	if len(tables) != 2 {
		t.Errorf("GetTables() returned %d tables, want 2", len(tables))
	}

	// Check table names
	tableNames := make([]string, len(tables))
	for i, table := range tables {
		tableNames[i] = table.Name
	}

	expectedNames := []string{"users", "posts"}
	if !reflect.DeepEqual(tableNames, expectedNames) {
		t.Errorf("GetTables() table names = %v, want %v", tableNames, expectedNames)
	}

	// Check first table details
	if tables[0].Name != "users" {
		t.Errorf("First table name = %v, want %v", tables[0].Name, "users")
	}
	if tables[0].RootPage != 2 {
		t.Errorf("First table root page = %v, want %v", tables[0].RootPage, 2)
	}
	if tables[0].SchemaSQL != "CREATE TABLE users(id INTEGER, name TEXT)" {
		t.Errorf("First table SQL = %v, want %v", tables[0].SchemaSQL, "CREATE TABLE users(id INTEGER, name TEXT)")
	}
}

func TestGetSchemaObjectsLogic(t *testing.T) {
	// Create mock database with known schema
	mockDB := &SQLiteDB{
		schemaTable: []*Cell{
			createMockSchemaCell("table", "users", "users", "CREATE TABLE users(id INTEGER, name TEXT)", 2),
			createMockSchemaCell("index", "idx_users_name", "users", "CREATE INDEX idx_users_name ON users(name)", 4),
			createMockSchemaCell("view", "user_view", "user_view", "CREATE VIEW user_view AS SELECT * FROM users", 0),
			createMockSchemaCell("trigger", "user_trigger", "users", "CREATE TRIGGER user_trigger AFTER INSERT ON users BEGIN END", 0),
		},
	}

	objects := mockDB.GetSchemaObjects()

	if len(objects) != 4 {
		t.Errorf("GetSchemaObjects() returned %d objects, want 4", len(objects))
	}

	// Check types
	expectedTypes := []string{"table", "index", "view", "trigger"}
	for i, obj := range objects {
		if obj.Type != expectedTypes[i] {
			t.Errorf("Object %d type = %v, want %v", i, obj.Type, expectedTypes[i])
		}
	}

	// Check specific object details
	tableObj := objects[0]
	if tableObj.Name != "users" || tableObj.TblName != "users" || tableObj.RootPage != 2 {
		t.Errorf("Table object details incorrect: %+v", tableObj)
	}

	indexObj := objects[1]
	if indexObj.Name != "idx_users_name" || indexObj.TblName != "users" || indexObj.RootPage != 4 {
		t.Errorf("Index object details incorrect: %+v", indexObj)
	}
}

func TestDatabaseHeaderFields(t *testing.T) {
	// Test that DatabaseHeader struct has expected fields
	header := &DatabaseHeader{
		PageSize:        4096,
		FileFormatWrite: 1,
		FileFormatRead:  1,
		DatabaseSize:    1024,
	}

	if header.PageSize != 4096 {
		t.Errorf("DatabaseHeader.PageSize = %v, want %v", header.PageSize, 4096)
	}
	if header.FileFormatWrite != 1 {
		t.Errorf("DatabaseHeader.FileFormatWrite = %v, want %v", header.FileFormatWrite, 1)
	}
}

func TestPageHeaderFields(t *testing.T) {
	// Test that PageHeader struct has expected fields
	pageHeader := &PageHeader{
		PageType:         0x0d, // Table B-Tree Leaf
		CellCount:        5,
		CellContentStart: 1000,
		FragmentedBytes:  0,
	}

	if pageHeader.PageType != 0x0d {
		t.Errorf("PageHeader.PageType = %v, want %v", pageHeader.PageType, 0x0d)
	}
	if pageHeader.CellCount != 5 {
		t.Errorf("PageHeader.CellCount = %v, want %v", pageHeader.CellCount, 5)
	}
}

// Benchmark tests
func BenchmarkReadVarint(b *testing.B) {
	data := []byte{0x81, 0x00} // Two-byte varint representing 128
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		readVarint(data, 0)
	}
}

func BenchmarkGetSerialTypeSize(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		getSerialTypeSize(SerialTypeInt32)
	}
}

func BenchmarkParseAsSchema(b *testing.B) {
	rb := &RecordBody{
		Values: []interface{}{
			[]byte("table"),
			[]byte("users"),
			[]byte("users"),
			[]byte{2},
			[]byte("CREATE TABLE users(id INTEGER, name TEXT)"),
		},
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rb.ParseAsSchema()
	}
}
