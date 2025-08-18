package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	// Available if you need it!
	"github.com/xwb1989/sqlparser"
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

	// Extract command arguments - if it starts with ".", it's a command, otherwise collect as string
	var command string
	var sqlArgs string

	if len(args) > 2 && args[2][0] == '.' {
		command = args[2]
	} else {
		// Combine all arguments starting from index 2 as SQL command string
		sqlArgs = strings.Join(args[2:], " ")
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

		// Handle SQL commands passed as arguments using sqlparser
		fmt.Fprintf(os.Stderr, "Processing SQL command with args: %v\n", sqlArgs)

		// Parse the SQL statement using sqlparser
		stmt, err := sqlparser.Parse(sqlArgs)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing SQL: %v\n", err)
			return fmt.Errorf("failed to parse SQL: %v", err)
		}

		// Handle different types of SQL statements
		switch parsedStmt := stmt.(type) {
		case *sqlparser.Select:
			// Extract information from the SELECT statement
			fmt.Fprintf(os.Stderr, "Found SELECT statement\n")

			// Extract table name from FROM clause
			tableName := extractTableName(parsedStmt)
			fmt.Fprintf(os.Stderr, "Parsed SELECT statement for table: %s\n", tableName)

			// Process the SELECT expressions using dedicated function
			err := processSelectExpressions(parsedStmt.SelectExprs, tableName, db)
			if err != nil {
				return err
			}

		case *sqlparser.Insert:
			fmt.Fprintf(os.Stderr, "Found INSERT statement\n")
			if parsedStmt.Table.Name.String() != "" {
				fmt.Fprintf(os.Stderr, "INSERT into table: %s\n", parsedStmt.Table.Name.String())
			}

		case *sqlparser.Update:
			fmt.Fprintf(os.Stderr, "Found UPDATE statement\n")
			if len(parsedStmt.TableExprs) > 0 {
				fmt.Fprintf(os.Stderr, "UPDATE table: %v\n", parsedStmt.TableExprs)
			}

		case *sqlparser.Delete:
			fmt.Fprintf(os.Stderr, "Found DELETE statement\n")
			if len(parsedStmt.TableExprs) > 0 {
				fmt.Fprintf(os.Stderr, "DELETE from table: %v\n", parsedStmt.TableExprs)
			}

		default:
			fmt.Fprintf(os.Stderr, "Unsupported SQL statement type: %T\n", parsedStmt)
			return fmt.Errorf("unsupported SQL statement")
		}

	default:
		fmt.Println("Unknown command", command)
		return fmt.Errorf("unknown command: %s", command)
	}

	return nil
}

// processSelectExpressions handles different types of SELECT expressions
func processSelectExpressions(selectExprs sqlparser.SelectExprs, tableName string, db *SQLiteDB) error {
	if selectExprs == nil {
		return nil
	}

	for _, expr := range selectExprs {
		switch selectExpr := expr.(type) {
		case *sqlparser.StarExpr:
			// Handle SELECT *
			fmt.Fprintf(os.Stderr, "Found SELECT * - need to return all columns\n")
			// TODO: Implement returning all columns

		case *sqlparser.AliasedExpr:
			// Check if it's a function call like COUNT(*) or a regular column
			switch innerExpr := selectExpr.Expr.(type) {
			case *sqlparser.FuncExpr:
				funcName := innerExpr.Name.String()
				fmt.Fprintf(os.Stderr, "Found function: %s\n", funcName)

				if strings.ToLower(funcName) == "count" {
					// Handle COUNT(*) function
					err := handleCountFunction(tableName, db)
					if err != nil {
						return err
					}
				}

			case *sqlparser.ColName:
				// Handle regular column name
				columnName := innerExpr.Name.String()
				fmt.Fprintf(os.Stderr, "Found column name: %s\n", columnName)
				fmt.Printf("%s\n", columnName)
				handleColumn(tableName, columnName, db)

			default:
				fmt.Fprintf(os.Stderr, "Unknown expression type in AliasedExpr: %T\n", innerExpr)
			}

		default:
			fmt.Fprintf(os.Stderr, "Unknown expression type in SELECT: %T\n", selectExpr)
		}
	}

	return nil
} // handleCountFunction processes COUNT(*) functions
func handleCountFunction(tableName string, db *SQLiteDB) error {
	table := db.GetTable(tableName)
	if table == nil {
		return fmt.Errorf("table %s not found", tableName)
	}

	db.file.Seek(int64(db.header.PageSize*(uint16(table.RootPage-1))), 0)
	pageHeader, err := db.readPageHeader()
	if err != nil {
		return err
	}
	fmt.Println(pageHeader.CellCount)
	return nil
}

// handleColumn
func handleColumn(tableName string, colName string, db *SQLiteDB) error {
	return nil
}

// extractTableName extracts the table name from a SELECT statement
func extractTableName(stmt *sqlparser.Select) string {
	if len(stmt.From) == 0 {
		return ""
	}

	switch tableExpr := stmt.From[0].(type) {
	case *sqlparser.AliasedTableExpr:
		switch table := tableExpr.Expr.(type) {
		case sqlparser.TableName:
			return table.Name.String()
		}
	}
	return ""
}

// Usage: your_program.sh sample.db .dbinfo
func main() {
	err := runProgram(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
