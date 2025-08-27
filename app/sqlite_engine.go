package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/xwb1989/sqlparser"
)

// SqliteEngine represents the main SQLite query engine
type SqliteEngine struct {
	db        Database
	formatter OutputFormatter
}

// NewSqliteEngine creates a new SQLite engine instance
func NewSqliteEngine(dbPath string) (*SqliteEngine, error) {
	db, err := NewDatabase(dbPath)
	if err != nil {
		return nil, err
	}

	formatter := NewConsoleFormatter(os.Stdout)

	return &SqliteEngine{
		db:        db,
		formatter: formatter,
	}, nil
}

// Close closes the SQLite engine
func (engine *SqliteEngine) Close() error {
	return engine.db.Close()
}

// ExecuteCommand executes a command
func (engine *SqliteEngine) ExecuteCommand(command, args string) error {
	switch command {
	case ".dbinfo":
		return engine.handleDBInfo()
	case ".tables":
		return engine.handleTables()
	case "sql":
		return engine.handleSQL(args)
	default:
		return fmt.Errorf("unknown command: %s", command)
	}
}

// handleDBInfo handles the .dbinfo command
func (engine *SqliteEngine) handleDBInfo() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get the actual page size from the database header
	pageSize := engine.db.GetPageSize()
	fmt.Printf("database page size: %v\n", pageSize)

	tables, err := engine.db.GetTables(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("number of tables: %v\n", len(tables)-1) // sqlite_schema table should not be counted
	return nil
}

// handleTables handles the .tables command
func (engine *SqliteEngine) handleTables() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tableNames, err := engine.db.GetTables(ctx)
	if err != nil {
		return err
	}
	for _, tableName := range tableNames {
		fmt.Printf("%s ", tableName)
	}
	fmt.Println()
	return nil
}

// handleSQL handles SQL commands
func (engine *SqliteEngine) handleSQL(sqlArgs string) error {
	stmt, err := sqlparser.Parse(sqlArgs)
	if err != nil {
		return fmt.Errorf("failed to parse SQL: %v", err)
	}

	switch parsedStmt := stmt.(type) {
	case *sqlparser.Select:
		return engine.handleSelect(parsedStmt)
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
func (engine *SqliteEngine) handleSelect(stmt *sqlparser.Select) error {
	tableName := engine.extractTableName(stmt)
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
		return engine.handleSelectAll(tableName, stmt.Where)
	} else if hasCountFunc {
		return engine.handleCount(tableName, stmt.Where)
	} else if len(columnNames) > 0 {
		// fmt.Println("++", strings.Join(columnNames, "|"))
		return engine.handleSelectColumns(tableName, columnNames, stmt.Where)
	}

	return fmt.Errorf("no valid columns found in SELECT statement")
}

// filterRows filters rows based on WHERE clause conditions
func (engine *SqliteEngine) filterRows(rows []*Row, schema []*Column, whereClause *sqlparser.Where) ([]*Row, error) {
	if whereClause == nil {
		return rows, nil
	}

	var filteredRows []*Row
	for _, row := range rows {
		match, err := engine.evaluateWhereCondition(row, schema, whereClause.Expr)
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
func (engine *SqliteEngine) evaluateWhereCondition(row *Row, schema []*Column, expr sqlparser.Expr) (bool, error) {
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		return engine.evaluateComparison(row, schema, e)
	case *sqlparser.AndExpr:
		left, err := engine.evaluateWhereCondition(row, schema, e.Left)
		if err != nil {
			return false, err
		}
		if !left {
			return false, nil // Short-circuit AND
		}
		return engine.evaluateWhereCondition(row, schema, e.Right)
	case *sqlparser.OrExpr:
		left, err := engine.evaluateWhereCondition(row, schema, e.Left)
		if err != nil {
			return false, err
		}
		if left {
			return true, nil // Short-circuit OR
		}
		return engine.evaluateWhereCondition(row, schema, e.Right)
	case *sqlparser.ParenExpr:
		return engine.evaluateWhereCondition(row, schema, e.Expr)
	default:
		return false, fmt.Errorf("unsupported WHERE expression type: %T", expr)
	}
}

// evaluateComparison evaluates a comparison expression (=, !=, <, >, <=, >=)
func (engine *SqliteEngine) evaluateComparison(row *Row, schema []*Column, comp *sqlparser.ComparisonExpr) (bool, error) {
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
	compValue, err := engine.extractComparisonValue(comp.Right)
	if err != nil {
		return false, fmt.Errorf("error extracting comparison value: %v", err)
	}

	// Perform comparison
	return engine.compareValues(rowValue, compValue, comp.Operator)
}

// extractComparisonValue extracts the value from the right side of a comparison
func (engine *SqliteEngine) extractComparisonValue(expr sqlparser.Expr) (interface{}, error) {
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
func (engine *SqliteEngine) compareValues(rowValue Value, compValue interface{}, operator string) (bool, error) {
	// Convert both values to strings for comparison
	rowStr := engine.formatter.FormatValue(rowValue)
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
func (engine *SqliteEngine) handleSelectAll(tableName string, whereClause *sqlparser.Where) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	table, err := engine.db.GetTable(ctx, tableName)
	if err != nil {
		return err
	}

	schema, err := table.GetSchema(ctx)
	if err != nil {
		return err
	}

	rows, err := table.GetRows(ctx)
	if err != nil {
		return err
	}

	// Convert []Column to []*Column for compatibility
	schemaPointers := make([]*Column, len(schema))
	for i := range schema {
		schemaPointers[i] = &schema[i]
	}

	// Convert []Row to []*Row for compatibility
	rowPointers := make([]*Row, len(rows))
	for i := range rows {
		rowPointers[i] = &rows[i]
	}

	// Apply WHERE clause filtering
	filteredRows, err := engine.filterRows(rowPointers, schemaPointers, whereClause)
	if err != nil {
		return err
	}

	output := engine.formatter.FormatTable(filteredRows, schemaPointers)
	fmt.Print(output)
	return nil
}

// handleSelectColumns handles SELECT column statements (single or multiple columns)
func (engine *SqliteEngine) handleSelectColumns(tableName string, columnNames []string, whereClause *sqlparser.Where) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get table instance
	table, err := engine.db.GetTable(ctx, tableName)
	if err != nil {
		return err
	}

	// Get table schema to validate columns and get their indices
	schema, err := table.GetSchema(ctx)
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
	rows, err := table.GetRows(ctx)
	if err != nil {
		return err
	}

	// Convert []Column to []*Column for compatibility
	schemaPointers := make([]*Column, len(schema))
	for i := range schema {
		schemaPointers[i] = &schema[i]
	}

	// Convert []Row to []*Row for compatibility
	rowPointers := make([]*Row, len(rows))
	for i := range rows {
		rowPointers[i] = &rows[i]
	}

	// Apply WHERE clause filtering
	filteredRows, err := engine.filterRows(rowPointers, schemaPointers, whereClause)
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
			rowValues[i] = engine.formatter.FormatValue(value)
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
func (engine *SqliteEngine) handleCount(tableName string, whereClause *sqlparser.Where) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	table, err := engine.db.GetTable(ctx, tableName)
	if err != nil {
		return err
	}

	if whereClause == nil {
		// If no WHERE clause, use the optimized row count method
		count, err := table.Count(ctx)
		if err != nil {
			return err
		}
		formatted := engine.formatter.FormatCount(count)
		fmt.Println(formatted)
		return nil
	}

	// If there's a WHERE clause, we need to fetch and filter rows
	schema, err := table.GetSchema(ctx)
	if err != nil {
		return err
	}

	rows, err := table.GetRows(ctx)
	if err != nil {
		return err
	}

	// Convert []Column to []*Column for compatibility
	schemaPointers := make([]*Column, len(schema))
	for i := range schema {
		schemaPointers[i] = &schema[i]
	}

	// Convert []Row to []*Row for compatibility
	rowPointers := make([]*Row, len(rows))
	for i := range rows {
		rowPointers[i] = &rows[i]
	}

	filteredRows, err := engine.filterRows(rowPointers, schemaPointers, whereClause)
	if err != nil {
		return err
	}

	count := len(filteredRows)
	formatted := engine.formatter.FormatCount(count)
	fmt.Println(formatted)
	return nil
}

// extractTableName extracts the table name from a SELECT statement
func (engine *SqliteEngine) extractTableName(stmt *sqlparser.Select) string {
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
