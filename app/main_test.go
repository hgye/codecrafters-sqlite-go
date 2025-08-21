package main

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"
)

func TestMainFunctionality(t *testing.T) {
	// Skip if sample.db doesn't exist
	dbPath := "../sample.db"
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Skip("sample.db not found, skipping main functionality test")
	}

	tests := []struct {
		name     string
		args     []string
		contains []string // Expected strings in output
	}{
		{
			name:     "dbinfo command",
			args:     []string{"test", dbPath, ".dbinfo"},
			contains: []string{"database page size:", "number of tables:"},
		},
		{
			name:     "tables command",
			args:     []string{"test", dbPath, ".tables"},
			contains: []string{"sqlite_master"},
		},
		{
			name:     "sql select count(*)",
			args:     []string{"test", dbPath, "SELECT", "COUNT(*)", "FROM", "oranges"},
			contains: []string{"6"}, // Assuming there are 6 rows in the oranges table
		},
		{
			name:     "sql command",
			args:     []string{"test", dbPath, "SELECT", "name", "FROM", "apples"},
			contains: []string{"Granny Smith", "Fuji", "Honeycrisp", "Golden Delicious"},
		},
		{
			name:     "sql select with multiple columns",
			args:     []string{"test", dbPath, "SELECT", "name, color", "FROM", "apples"},
			contains: []string{"Granny Smith|Light Green", "Fuji|Red", "Honeycrisp|Blush Red", "Golden Delicious|Yellow"},
		},
		{
			name:     "sql select with where clause",
			args:     []string{"test", dbPath, "SELECT", "name, color", "FROM", "apples", "WHERE", "color = 'Red'"},
			contains: []string{"Fuji|Red"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Capture output
			oldStdout := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w

			// Run the program function with our test arguments
			go func() {
				defer func() {
					if r := recover(); r != nil {
						// Handle any panics
					}
				}()
				runProgram(tt.args)
				w.Close()
			}()

			// Read output
			outputBytes, _ := io.ReadAll(r)
			output := string(outputBytes)

			// Restore stdout
			os.Stdout = oldStdout

			// Check output contains expected strings
			for _, expected := range tt.contains {
				if !strings.Contains(output, expected) {
					t.Errorf("Output should contain '%s', got: %s", expected, output)
				}
			}
		})
	}
}

func TestMainWithInvalidArgs(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "no arguments",
			args: []string{"test"},
		},
		{
			name: "only database path",
			args: []string{"test", "sample.db"},
		},
		{
			name: "nonexistent database",
			args: []string{"test", "/nonexistent/database.db", ".dbinfo"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Capture output
			oldStdout := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w

			// Run the program function with invalid arguments
			err := runProgram(tt.args)
			w.Close()

			// Read output
			outputBytes, _ := io.ReadAll(r)
			outputStr := string(outputBytes)

			// Restore stdout
			os.Stdout = oldStdout

			// For invalid args, we expect an error and some output
			if err == nil {
				t.Errorf("Expected an error for invalid args, got nil")
			}

			if len(outputStr) == 0 {
				t.Errorf("Expected some output for invalid args, got empty string")
			}

			// Should contain either "Usage:" or error message about file not existing
			if !strings.Contains(outputStr, "Usage:") && !strings.Contains(outputStr, "does not exist") {
				t.Logf("Output for invalid args: %s", outputStr)
			}
		})
	}
}

// Integration test that tests the entire flow
func TestSQLiteParsingIntegration(t *testing.T) {
	// Skip if sample.db doesn't exist
	dbPath := "../sample.db"
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Skip("sample.db not found, skipping integration test")
	}

	// Test complete parsing flow
	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Test schema parsing
	ctx := context.Background()
	schema, err := db.GetSchema(ctx)
	if err != nil {
		t.Fatalf("Failed to get schema: %v", err)
	}
	if len(schema) == 0 {
		t.Fatal("No schema objects found")
	}

	// Parse each schema object
	for _, schemaRecord := range schema {
		// Validate schema record fields
		if schemaRecord.Type == "" {
			t.Errorf("Schema record missing type")
		}
		if schemaRecord.Name == "" {
			t.Errorf("Schema record missing name")
		}
	}

	// Test table creation from schema
	tables, err := db.GetTables(ctx)
	if err != nil {
		t.Fatalf("Failed to get tables: %v", err)
	}
	for _, tableName := range tables {
		if tableName == "" {
			t.Errorf("Table missing name")
		}
	}

	// Test schema objects include all types
	objects, err := db.GetSchema(ctx)
	if err != nil {
		t.Fatalf("Failed to get schema: %v", err)
	}
	typesSeen := make(map[string]bool)
	for _, obj := range objects {
		typesSeen[obj.Type] = true
	}

	// Should at least have tables
	if !typesSeen["table"] {
		t.Errorf("Should have at least one table in schema objects")
	}
}

// Test the record body serialization functionality
func TestRecordBodySerialization(t *testing.T) {
	// Create a record body with known values
	rb := &RecordBody{
		Values: []interface{}{
			[]byte("table"),
			[]byte("test_table"),
			[]byte("test_table"),
			[]byte{5},
			[]byte("CREATE TABLE test_table(id INTEGER, name TEXT)"),
		},
	}

	// Test parsing
	schema := rb.ParseAsSchema()
	if schema == nil {
		t.Fatal("Failed to parse schema")
	}

	// Test all fields
	if schema.Type != "table" {
		t.Errorf("Type = %v, want %v", schema.Type, "table")
	}
	if schema.Name != "test_table" {
		t.Errorf("Name = %v, want %v", schema.Name, "test_table")
	}
	if schema.TblName != "test_table" {
		t.Errorf("TblName = %v, want %v", schema.TblName, "test_table")
	}
	if schema.RootPage != 5 {
		t.Errorf("RootPage = %v, want %v", schema.RootPage, 5)
	}
	if schema.SQL != "CREATE TABLE test_table(id INTEGER, name TEXT)" {
		t.Errorf("SQL = %v, want expected CREATE statement", schema.SQL)
	}

	// Test union field is set
	if rb.Schema != schema {
		t.Errorf("Union field not set correctly")
	}

	// Test identification
	if !rb.IsSchemaRecord() {
		t.Errorf("IsSchemaRecord() should return true")
	}
}
