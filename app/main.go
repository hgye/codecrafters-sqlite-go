package main

import (
	"fmt"
	"log"
	"os"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

// runProgram handles the core logic, separated from main for testability
func runProgram(args []string) error {
	if len(args) < 3 {
		fmt.Println("Usage: your_program.sh <database_file> <command>")
		return fmt.Errorf("insufficient arguments")
	}

	databaseFilePath := args[1]

	if _, err := os.Stat(databaseFilePath); os.IsNotExist(err) {
		fmt.Printf("Database file %s does not exist\n", databaseFilePath)
		return fmt.Errorf("database file does not exist: %s", databaseFilePath)
	}

	// Extract command arguments - if it starts with ".", it's a command, otherwise collect as string array
	var command string
	var sqlArgs []string

	if len(args) > 2 && args[2][0] == '.' {
		command = args[2]
	} else {
		// Combine all arguments starting from index 2 as SQL command parts
		sqlArgs = args[2:]
		command = "sql" // Use "sql" as a generic command type
	}

	// Open the SQLite database using the new structure
	db, err := NewSQLiteDB(databaseFilePath)
	if err != nil {
		return err
	}
	defer db.Close()

	switch command {
	case ".dbinfo":
		fmt.Printf("database page size: %v\n", db.GetPageSize())
		fmt.Printf("number of tables: %v\n", db.GetTableCount())

	case ".tables":
		// Get table names from the schema
		tableNames := db.GetTableNames()
		for _, tableName := range tableNames {
			fmt.Printf("%s ", tableName)
		}
		fmt.Println()

	case "sql":
		// Handle SQL commands passed as arguments
		fmt.Printf("Processing SQL command with args: %v\n", sqlArgs)

		// Schema is accessible globally through db.GetSchema()
		schemaTables := db.GetSchema()
		fmt.Printf("Schema tables available: %d\n", len(schemaTables))

		// TODO: Use schemaTables for SQL query processing
		// For now, just echo the arguments
		for i, arg := range sqlArgs {
			fmt.Printf("Arg %d: %s\n", i, arg)
		}

	default:
		fmt.Println("Unknown command", command)
		return fmt.Errorf("unknown command: %s", command)
	}

	return nil
}

// Usage: your_program.sh sample.db .dbinfo
func main() {
	err := runProgram(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
