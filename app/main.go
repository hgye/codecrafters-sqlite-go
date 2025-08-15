package main

import (
	"fmt"
	"log"
	"os"
	"strings"
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
		fmt.Fprintf(os.Stderr, "Processing SQL command with args: %v\n", sqlArgs)

		// Join args with spaces and then split properly
		sqlString := strings.Join(sqlArgs, " ")

		// Check and remove trailing semicolon
		if strings.HasSuffix(sqlString, ";") {
			sqlString = strings.TrimSuffix(sqlString, ";")
			fmt.Println("Removed trailing semicolon")
		}

		// Split back into individual words
		sqlParts := strings.Fields(sqlString)
		fmt.Fprintf(os.Stderr, "SQL parts after processing: %v\n", sqlParts)

		// Use schemaTables for SQL query processing
		table_name := sqlParts[len(sqlParts)-1] // Example: last argument as table name
		tables := db.GetTables()

		fmt.Fprintf(os.Stderr, "table_name is %s\n", table_name)

		for _, t := range tables {
			if t.Name == table_name {
				// fmt.Println(t.RootPage)
				db.file.Seek(int64(db.header.PageSize*(uint16(t.RootPage-1))), 0)
				pageHeader, err := db.readPageHeader()
				if err != nil {
					return err
				}
				fmt.Println(pageHeader.CellCount)
				break
			}
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
