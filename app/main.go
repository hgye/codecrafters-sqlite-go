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

	var columnNames []string
	var hasStarExpr bool
	var hasCountFunc bool

	// First pass: collect all column names and check for special cases
	for _, expr := range stmt.SelectExprs {
		switch selectExpr := expr.(type) {
		case *sqlparser.StarExpr:
			hasStarExpr = true
		case *sqlparser.AliasedExpr:
			switch innerExpr := selectExpr.Expr.(type) {
			case *sqlparser.FuncExpr:
				funcName := strings.ToLower(innerExpr.Name.String())
				if funcName == "count" {
					hasCountFunc = true
				} else {
					return fmt.Errorf("unsupported function: %s", funcName)
				}
			case *sqlparser.ColName:
				columnName := innerExpr.Name.String()
				columnNames = append(columnNames, columnName)
			default:
				return fmt.Errorf("unsupported expression type: %T", innerExpr)
			}
		default:
			return fmt.Errorf("unsupported SELECT expression type: %T", selectExpr)
		}
	}

	// Handle different cases with WHERE clause support
	if hasStarExpr {
		return app.handleSelectAll(tableName, stmt.Where)
	} else if hasCountFunc {
		return app.handleCount(tableName, stmt.Where)
	} else if len(columnNames) > 0 {
		return app.handleSelectColumns(tableName, columnNames, stmt.Where)
	}

	return fmt.Errorf("no valid columns found in SELECT statement")
}

// filterRows filters rows based on WHERE clause conditions
func (app *Application) filterRows(rows []*Row, schema []*Column, whereClause *sqlparser.Where) ([]*Row, error) {
	if whereClause == nil {
		return rows, nil
	}

	var filteredRows []*Row
	for _, row := range rows {
		match, err := app.evaluateWhereCondition(row, schema, whereClause.Expr)
		if err != nil {
			return nil, fmt.Errorf("error evaluating WHERE condition: %v", err)
		}
		if match {
			filteredRows = append(filteredRows, row)
		}
	}
	return filteredRows, nil
}

// evaluateWhereCondition evaluates a WHERE condition for a single row
func (app *Application) evaluateWhereCondition(row *Row, schema []*Column, expr sqlparser.Expr) (bool, error) {
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		return app.evaluateComparison(row, schema, e)
	case *sqlparser.AndExpr:
		left, err := app.evaluateWhereCondition(row, schema, e.Left)
		if err != nil {
			return false, err
		}
		if !left {
			return false, nil // Short-circuit AND
		}
		return app.evaluateWhereCondition(row, schema, e.Right)
	case *sqlparser.OrExpr:
		left, err := app.evaluateWhereCondition(row, schema, e.Left)
		if err != nil {
			return false, err
		}
		if left {
			return true, nil // Short-circuit OR
		}
		return app.evaluateWhereCondition(row, schema, e.Right)
	case *sqlparser.ParenExpr:
		return app.evaluateWhereCondition(row, schema, e.Expr)
	default:
		return false, fmt.Errorf("unsupported WHERE expression type: %T", expr)
	}
}

// evaluateComparison evaluates a comparison expression (=, !=, <, >, <=, >=)
func (app *Application) evaluateComparison(row *Row, schema []*Column, comp *sqlparser.ComparisonExpr) (bool, error) {
	// Get column name and value
	colName, ok := comp.Left.(*sqlparser.ColName)
	if !ok {
		return false, fmt.Errorf("left side of comparison must be a column name")
	}

	// Find column index
	columnName := colName.Name.String()
	columnIndex := -1
	for _, col := range schema {
		if strings.EqualFold(col.Name, columnName) {
			columnIndex = col.Index
			break
		}
	}
	if columnIndex == -1 {
		return false, fmt.Errorf("column '%s' not found", columnName)
	}

	// Get row value
	rowValue, err := row.Get(columnIndex)
	if err != nil {
		return false, fmt.Errorf("error getting row value: %v", err)
	}

	// Get comparison value
	compValue, err := app.extractComparisonValue(comp.Right)
	if err != nil {
		return false, fmt.Errorf("error extracting comparison value: %v", err)
	}

	// Perform comparison
	return app.compareValues(rowValue, compValue, comp.Operator)
}

// extractComparisonValue extracts the value from the right side of a comparison
func (app *Application) extractComparisonValue(expr sqlparser.Expr) (interface{}, error) {
	switch e := expr.(type) {
	case *sqlparser.SQLVal:
		switch e.Type {
		case sqlparser.StrVal:
			return string(e.Val), nil
		case sqlparser.IntVal:
			return string(e.Val), nil
		case sqlparser.FloatVal:
			return string(e.Val), nil
		default:
			return string(e.Val), nil
		}
	case *sqlparser.ColName:
		return e.Name.String(), nil
	default:
		return nil, fmt.Errorf("unsupported comparison value type: %T", expr)
	}
}

// compareValues compares two values based on the operator
func (app *Application) compareValues(rowValue Value, compValue interface{}, operator string) (bool, error) {
	// Convert both values to strings for comparison
	rowStr := app.formatter.FormatValue(rowValue)
	compStr := fmt.Sprintf("%v", compValue)

	switch operator {
	case "=":
		return rowStr == compStr, nil
	case "!=", "<>":
		return rowStr != compStr, nil
	case "<":
		return rowStr < compStr, nil
	case "<=":
		return rowStr <= compStr, nil
	case ">":
		return rowStr > compStr, nil
	case ">=":
		return rowStr >= compStr, nil
	default:
		return false, fmt.Errorf("unsupported operator: %s", operator)
	}
}

// handleSelectAll handles SELECT * statements
func (app *Application) handleSelectAll(tableName string, whereClause *sqlparser.Where) error {
	schema, err := app.db.GetTableSchema(tableName)
	if err != nil {
		return err
	}

	rows, err := app.db.GetTableRows(tableName)
	if err != nil {
		return err
	}

	// Apply WHERE clause filtering
	filteredRows, err := app.filterRows(rows, schema, whereClause)
	if err != nil {
		return err
	}

	output := app.formatter.FormatTable(filteredRows, schema)
	fmt.Print(output)
	return nil
}

// handleSelectColumns handles SELECT column statements (single or multiple columns)
func (app *Application) handleSelectColumns(tableName string, columnNames []string, whereClause *sqlparser.Where) error {
	// Get table schema to validate columns and get their indices
	schema, err := app.db.GetTableSchema(tableName)
	if err != nil {
		return err
	}

	// Create a map for quick column index lookup
	columnIndexMap := make(map[string]int)
	for _, col := range schema {
		columnIndexMap[strings.ToLower(col.Name)] = col.Index
	}

	// Validate all requested columns exist and get their indices
	columnIndices := make([]int, len(columnNames))
	for i, columnName := range columnNames {
		if index, exists := columnIndexMap[strings.ToLower(columnName)]; exists {
			columnIndices[i] = index
		} else {
			return fmt.Errorf("column '%s' not found in table '%s'", columnName, tableName)
		}
	}

	// Get all rows
	rows, err := app.db.GetTableRows(tableName)
	if err != nil {
		return err
	}

	// Apply WHERE clause filtering
	filteredRows, err := app.filterRows(rows, schema, whereClause)
	if err != nil {
		return err
	}

	// Output the selected columns for each row
	for _, row := range filteredRows {
		rowValues := make([]string, len(columnIndices))
		for i, colIndex := range columnIndices {
			value, err := row.Get(colIndex)
			if err != nil {
				return fmt.Errorf("error getting value for column index %d: %v", colIndex, err)
			}
			rowValues[i] = app.formatter.FormatValue(value)
		}

		// For single column, print one per line; for multiple columns, print tab-separated
		if len(rowValues) == 1 {
			fmt.Println(rowValues[0])
		} else {
			fmt.Println(strings.Join(rowValues, "|"))
		}
	}

	return nil
}

// handleCount handles COUNT(*) statements
func (app *Application) handleCount(tableName string, whereClause *sqlparser.Where) error {
	if whereClause == nil {
		// If no WHERE clause, use the optimized row count method
		count, err := app.db.GetRowCount(tableName)
		if err != nil {
			return err
		}
		formatted := app.formatter.FormatCount(count)
		fmt.Println(formatted)
		return nil
	}

	// If there's a WHERE clause, we need to fetch and filter rows
	schema, err := app.db.GetTableSchema(tableName)
	if err != nil {
		return err
	}

	rows, err := app.db.GetTableRows(tableName)
	if err != nil {
		return err
	}

	filteredRows, err := app.filterRows(rows, schema, whereClause)
	if err != nil {
		return err
	}

	count := len(filteredRows)
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
