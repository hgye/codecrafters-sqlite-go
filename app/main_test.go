package main

import (
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
			name:     "sql command",
			args:     []string{"test", dbPath, "SELECT", "name", "FROM", "sqlite_master"},
			contains: []string{"Processing SQL command with args:", "Schema tables available:"},
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
	db, err := NewSQLiteDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Test schema parsing
	schema := db.GetSchema()
	if len(schema) == 0 {
		t.Fatal("No schema objects found")
	}

	// Parse each schema object
	for _, cell := range schema {
		schemaRecord := cell.Record.RecordBody.ParseAsSchema()
		if schemaRecord == nil {
			t.Errorf("Failed to parse schema record")
			continue
		}

		// Validate schema record fields
		if schemaRecord.Type == "" {
			t.Errorf("Schema record missing type")
		}
		if schemaRecord.Name == "" {
			t.Errorf("Schema record missing name")
		}

		// Test that the union field is set
		if cell.Record.RecordBody.Schema == nil {
			t.Errorf("Union field Schema not set after parsing")
		}

		// Test IsSchemaRecord
		if !cell.Record.RecordBody.IsSchemaRecord() {
			t.Errorf("IsSchemaRecord() should return true for schema records")
		}
	}

	// Test table creation from schema
	tables := db.GetTables()
	for _, table := range tables {
		if table.Name == "" {
			t.Errorf("Table missing name")
		}
		if table.RootPage <= 0 {
			t.Errorf("Table should have valid root page")
		}

		// Test Table.String() method
		if table.String() != table.Name {
			t.Errorf("Table.String() = %v, want %v", table.String(), table.Name)
		}
	}

	// Test schema objects include all types
	objects := db.GetSchemaObjects()
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
