package main

import (
	"reflect"
	"testing"
)

func TestReadVarint(t *testing.T) {
	tests := []struct {
		name         string
		data         []byte
		offset       int
		expectedVal  uint64
		expectedRead int
	}{
		{
			name:         "single byte varint",
			data:         []byte{0x7F},
			offset:       0,
			expectedVal:  127,
			expectedRead: 1,
		},
		{
			name:         "two byte varint",
			data:         []byte{0x81, 0x00},
			offset:       0,
			expectedVal:  128,
			expectedRead: 2,
		},
		{
			name:         "zero value",
			data:         []byte{0x00},
			offset:       0,
			expectedVal:  0,
			expectedRead: 1,
		},
		{
			name:         "varint with offset",
			data:         []byte{0xFF, 0xFF, 0x7F},
			offset:       2,
			expectedVal:  127,
			expectedRead: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, bytesRead := readVarint(tt.data, tt.offset)
			if val != tt.expectedVal {
				t.Errorf("readVarint() value = %v, want %v", val, tt.expectedVal)
			}
			if bytesRead != tt.expectedRead {
				t.Errorf("readVarint() bytesRead = %v, want %v", bytesRead, tt.expectedRead)
			}
		})
	}
}

func TestGetSerialTypeSize(t *testing.T) {
	tests := []struct {
		serialType   uint8
		expectedSize int
	}{
		{SerialTypeNull, 0},
		{SerialTypeInt8, 1},
		{SerialTypeInt16, 2},
		{SerialTypeInt24, 3},
		{SerialTypeInt32, 4},
		{SerialTypeInt48, 6},
		{SerialTypeInt64, 8},
		{SerialTypeFloat64, 8},
		{SerialTypeZero, 0},
		{SerialTypeOne, 0},
		{12, 0}, // BLOB with 0 bytes: (12-12)/2 = 0
		{14, 1}, // BLOB with 1 byte: (14-12)/2 = 1
		{13, 0}, // TEXT with 0 bytes: (13-13)/2 = 0
		{15, 1}, // TEXT with 1 byte: (15-13)/2 = 1
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			size := getSerialTypeSize(tt.serialType)
			if size != tt.expectedSize {
				t.Errorf("getSerialTypeSize(%d) = %v, want %v", tt.serialType, size, tt.expectedSize)
			}
		})
	}
}

func TestRecordBodyParseAsSchema(t *testing.T) {
	tests := []struct {
		name           string
		values         []interface{}
		expectedSchema *SchemaRecord
		expectNil      bool
	}{
		{
			name: "valid table schema",
			values: []interface{}{
				[]byte("table"),
				[]byte("users"),
				[]byte("users"),
				[]byte{2}, // root page 2
				[]byte("CREATE TABLE users(id INTEGER, name TEXT)"),
			},
			expectedSchema: &SchemaRecord{
				Type:     "table",
				Name:     "users",
				TblName:  "users",
				RootPage: 2,
				SQL:      "CREATE TABLE users(id INTEGER, name TEXT)",
			},
			expectNil: false,
		},
		{
			name: "valid index schema",
			values: []interface{}{
				[]byte("index"),
				[]byte("idx_users_name"),
				[]byte("users"),
				[]byte{3}, // root page 3
				[]byte("CREATE INDEX idx_users_name ON users(name)"),
			},
			expectedSchema: &SchemaRecord{
				Type:     "index",
				Name:     "idx_users_name",
				TblName:  "users",
				RootPage: 3,
				SQL:      "CREATE INDEX idx_users_name ON users(name)",
			},
			expectNil: false,
		},
		{
			name:      "insufficient values",
			values:    []interface{}{[]byte("table"), []byte("users")},
			expectNil: true,
		},
		{
			name: "with nil values",
			values: []interface{}{
				[]byte("table"),
				nil,
				[]byte("users"),
				[]byte{2},
				nil,
			},
			expectedSchema: &SchemaRecord{
				Type:     "table",
				Name:     "",
				TblName:  "users",
				RootPage: 2,
				SQL:      "",
			},
			expectNil: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rb := &RecordBody{Values: tt.values}
			schema := rb.ParseAsSchema()

			if tt.expectNil {
				if schema != nil {
					t.Errorf("ParseAsSchema() expected nil, got %v", schema)
				}
				return
			}

			if schema == nil {
				t.Errorf("ParseAsSchema() got nil, expected %v", tt.expectedSchema)
				return
			}

			if !reflect.DeepEqual(schema, tt.expectedSchema) {
				t.Errorf("ParseAsSchema() = %v, want %v", schema, tt.expectedSchema)
			}

			// Check that union field is set
			if rb.Schema != schema {
				t.Errorf("Union field Schema not set correctly")
			}
		})
	}
}

func TestRecordBodyIsSchemaRecord(t *testing.T) {
	tests := []struct {
		name     string
		values   []interface{}
		expected bool
	}{
		{
			name: "valid table record",
			values: []interface{}{
				[]byte("table"),
				[]byte("users"),
				[]byte("users"),
				[]byte{2},
				[]byte("CREATE TABLE users(id INTEGER, name TEXT)"),
			},
			expected: true,
		},
		{
			name: "valid index record",
			values: []interface{}{
				[]byte("index"),
				[]byte("idx_name"),
				[]byte("users"),
				[]byte{3},
				[]byte("CREATE INDEX idx_name ON users(name)"),
			},
			expected: true,
		},
		{
			name: "valid view record",
			values: []interface{}{
				[]byte("view"),
				[]byte("user_view"),
				[]byte("user_view"),
				[]byte{0},
				[]byte("CREATE VIEW user_view AS SELECT * FROM users"),
			},
			expected: true,
		},
		{
			name: "valid trigger record",
			values: []interface{}{
				[]byte("trigger"),
				[]byte("user_trigger"),
				[]byte("users"),
				[]byte{0},
				[]byte("CREATE TRIGGER user_trigger AFTER INSERT ON users BEGIN END"),
			},
			expected: true,
		},
		{
			name: "invalid type",
			values: []interface{}{
				[]byte("unknown"),
				[]byte("something"),
				[]byte("something"),
				[]byte{1},
				[]byte("CREATE SOMETHING"),
			},
			expected: false,
		},
		{
			name: "wrong number of values",
			values: []interface{}{
				[]byte("table"),
				[]byte("users"),
			},
			expected: false,
		},
		{
			name: "nil first value",
			values: []interface{}{
				nil,
				[]byte("users"),
				[]byte("users"),
				[]byte{2},
				[]byte("CREATE TABLE users(id INTEGER, name TEXT)"),
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rb := &RecordBody{Values: tt.values}
			result := rb.IsSchemaRecord()
			if result != tt.expected {
				t.Errorf("IsSchemaRecord() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestNewTable(t *testing.T) {
	// Mock SQLiteDB for testing
	mockDB := &SQLiteDB{}

	table := NewTable("users", "CREATE TABLE users(id INTEGER, name TEXT)", 2, mockDB)

	if table.Name != "users" {
		t.Errorf("NewTable() Name = %v, want %v", table.Name, "users")
	}
	if table.SchemaSQL != "CREATE TABLE users(id INTEGER, name TEXT)" {
		t.Errorf("NewTable() SchemaSQL = %v, want %v", table.SchemaSQL, "CREATE TABLE users(id INTEGER, name TEXT)")
	}
	if table.RootPage != 2 {
		t.Errorf("NewTable() RootPage = %v, want %v", table.RootPage, 2)
	}
	if table.db != mockDB {
		t.Errorf("NewTable() db reference not set correctly")
	}
}

func TestNewTableFromSchemaCell(t *testing.T) {
	mockDB := &SQLiteDB{}

	tests := []struct {
		name        string
		cell        *Cell
		expectTable bool
		tableName   string
	}{
		{
			name: "valid table cell",
			cell: &Cell{
				Record: Record{
					RecordBody: RecordBody{
						Values: []interface{}{
							[]byte("table"),
							[]byte("users"),
							[]byte("users"),
							[]byte{2},
							[]byte("CREATE TABLE users(id INTEGER, name TEXT)"),
						},
					},
				},
			},
			expectTable: true,
			tableName:   "users",
		},
		{
			name: "index cell (not a table)",
			cell: &Cell{
				Record: Record{
					RecordBody: RecordBody{
						Values: []interface{}{
							[]byte("index"),
							[]byte("idx_name"),
							[]byte("users"),
							[]byte{3},
							[]byte("CREATE INDEX idx_name ON users(name)"),
						},
					},
				},
			},
			expectTable: false,
		},
		{
			name: "invalid cell (insufficient values)",
			cell: &Cell{
				Record: Record{
					RecordBody: RecordBody{
						Values: []interface{}{
							[]byte("table"),
							[]byte("users"),
						},
					},
				},
			},
			expectTable: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := NewTableFromSchemaCell(tt.cell, mockDB)

			if tt.expectTable {
				if table == nil {
					t.Errorf("NewTableFromSchemaCell() expected table, got nil")
					return
				}
				if table.Name != tt.tableName {
					t.Errorf("NewTableFromSchemaCell() table name = %v, want %v", table.Name, tt.tableName)
				}
			} else {
				if table != nil {
					t.Errorf("NewTableFromSchemaCell() expected nil, got %v", table)
				}
			}
		})
	}
}

func TestTableString(t *testing.T) {
	table := &Table{Name: "users"}
	expected := "users"

	if table.String() != expected {
		t.Errorf("Table.String() = %v, want %v", table.String(), expected)
	}
}
