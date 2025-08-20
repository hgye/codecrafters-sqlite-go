package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	// Available if you need it!
	"github.com/xwb1989/sqlparser"
)

// Application represents the main application
type Application struct {
	db        *SQLiteDB
	formatter OutputFormatter
}

// NewApplication creates a new application instance
func NewApplication(dbPath string) (*Application, error) {
	db, err := NewSQLiteDB(dbPath)
	if err != nil {
		return nil, err
	}

	formatter := NewConsoleFormatter(os.Stdout)

	return &Application{
		db:        db,
		formatter: formatter,
	}, nil
}

// Close closes the application
func (app *Application) Close() error {
	return app.db.Close()
}

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

	app, err := NewApplication(databaseFilePath)
	if err != nil {
		return err
	}
	defer app.Close()

	// Extract command arguments
	var command string
	var sqlArgs string

	if len(args) > 2 && args[2][0] == '.' {
		command = args[2]
	} else {
		sqlArgs = strings.Join(args[2:], " ")
		command = "sql"
	}

	return app.ExecuteCommand(command, sqlArgs)
}

// ExecuteCommand executes a command
func (app *Application) ExecuteCommand(command, args string) error {
	switch command {
	case ".dbinfo":
		return app.handleDBInfo()
	case ".tables":
		return app.handleTables()
	case "sql":
		return app.handleSQL(args)
	default:
		return fmt.Errorf("unknown command: %s", command)
	}
}

// handleDBInfo handles the .dbinfo command
func (app *Application) handleDBInfo() error {
	fmt.Printf("database page size: %v\n", app.db.GetPageSize())
	fmt.Printf("number of tables: %v\n", app.db.GetTableCount())
	return nil
}

// handleTables handles the .tables command
func (app *Application) handleTables() error {
	tableNames := app.db.GetTableNames()
	for _, tableName := range tableNames {
		fmt.Printf("%s ", tableName)
	}
	fmt.Println()
	return nil
}

// handleSQL handles SQL commands
func (app *Application) handleSQL(sqlArgs string) error {
	stmt, err := sqlparser.Parse(sqlArgs)
	if err != nil {
		return fmt.Errorf("failed to parse SQL: %v", err)
	}

	switch parsedStmt := stmt.(type) {
	case *sqlparser.Select:
		return app.handleSelect(parsedStmt)
	case *sqlparser.Insert:
		return fmt.Errorf("INSERT statements not supported yet")
	case *sqlparser.Update:
		return fmt.Errorf("UPDATE statements not supported yet")
	case *sqlparser.Delete:
		return fmt.Errorf("DELETE statements not supported yet")
	default:
		return fmt.Errorf("unsupported SQL statement type: %T", parsedStmt)
	}
}

// handleSelect handles SELECT statements
func (app *Application) handleSelect(stmt *sqlparser.Select) error {
	tableName := app.extractTableName(stmt)
	if tableName == "" {
		return fmt.Errorf("could not extract table name from SELECT statement")
	}

	// Process SELECT expressions
	for _, expr := range stmt.SelectExprs {
		switch selectExpr := expr.(type) {
		case *sqlparser.StarExpr:
			return app.handleSelectAll(tableName)
		case *sqlparser.AliasedExpr:
			switch innerExpr := selectExpr.Expr.(type) {
			case *sqlparser.FuncExpr:
				funcName := strings.ToLower(innerExpr.Name.String())
				if funcName == "count" {
					return app.handleCount(tableName)
				}
				return fmt.Errorf("unsupported function: %s", funcName)
			case *sqlparser.ColName:
				columnName := innerExpr.Name.String()
				fmt.Println("Column Name:", columnName)
				return app.handleSelectColumn(tableName, columnName)
			default:
				return fmt.Errorf("unsupported expression type: %T", innerExpr)
			}
		default:
			return fmt.Errorf("unsupported SELECT expression type: %T", selectExpr)
		}
	}

	return nil
}

// handleSelectAll handles SELECT * statements
func (app *Application) handleSelectAll(tableName string) error {
	schema, err := app.db.GetTableSchema(tableName)
	if err != nil {
		return err
	}

	rows, err := app.db.GetTableRows(tableName)
	if err != nil {
		return err
	}

	output := app.formatter.FormatTable(rows, schema)
	fmt.Print(output)
	return nil
}

// handleSelectColumn handles SELECT column statements
func (app *Application) handleSelectColumn(tableName, columnName string) error {
	values, err := app.db.GetColumnValues(tableName, columnName)
	if err != nil {
		return err
	}

	for _, value := range values {
		formatted := app.formatter.FormatValue(value)
		fmt.Println(formatted)
	}

	return nil
}

// handleCount handles COUNT(*) statements
func (app *Application) handleCount(tableName string) error {
	count, err := app.db.GetRowCount(tableName)
	if err != nil {
		return err
	}

	formatted := app.formatter.FormatCount(count)
	fmt.Println(formatted)
	return nil
}

// extractTableName extracts the table name from a SELECT statement
func (app *Application) extractTableName(stmt *sqlparser.Select) string {
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

// main function
func main() {
	err := runProgram(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
