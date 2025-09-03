package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

// QueryOptimizer handles query optimization using indexes
type QueryOptimizer struct {
	database Database
}

// NewQueryOptimizer creates a new query optimizer
func NewQueryOptimizer(db Database) *QueryOptimizer {
	return &QueryOptimizer{database: db}
}

// OptimizeSelect attempts to optimize a SELECT query using indexes
func (qo *QueryOptimizer) OptimizeSelect(query *sqlparser.Select) (*QueryPlan, error) {
	plan := &QueryPlan{
		QueryType: "SELECT",
		UseIndex:  false,
	}

	// Extract table name
	tableName := ""
	if len(query.From) > 0 {
		if tableExpr, ok := query.From[0].(*sqlparser.AliasedTableExpr); ok {
			if table, ok := tableExpr.Expr.(sqlparser.TableName); ok {
				tableName = table.Name.String()
			}
		}
	}

	if tableName == "" {
		return plan, fmt.Errorf("no table found in query")
	}

	plan.TableName = tableName

	// Get table from database
	ctx := context.Background()
	table, err := qo.database.GetTable(ctx, tableName)
	if err != nil {
		return plan, err
	}

	// Check if there's a WHERE clause
	if query.Where == nil {
		plan.UseIndex = false
		return plan, nil
	}

	// Try to find an index that can be used
	indexName, indexValue, err := qo.analyzeWhereClause(query.Where.Expr, table, ctx)
	if err != nil {
		plan.UseIndex = false
		return plan, nil // Not an error, just can't use index
	}

	if indexName != "" {
		plan.UseIndex = true
		plan.IndexName = indexName
		plan.IndexValue = indexValue
	}

	return plan, nil
}

// analyzeWhereClause analyzes the WHERE clause to find potential index usage
func (qo *QueryOptimizer) analyzeWhereClause(expr sqlparser.Expr, table Table, ctx context.Context) (string, interface{}, error) {
	switch node := expr.(type) {
	case *sqlparser.ComparisonExpr:
		if node.Operator == "=" {
			// Check if left side is a column name
			if colName, ok := node.Left.(*sqlparser.ColName); ok {
				columnName := colName.Name.String()

				// Get available indexes for this table
				indexes, err := table.GetIndexes(ctx)
				if err != nil {
					return "", nil, err
				}

				// Check if there's an index on this column
				for _, index := range indexes {
					// Get index schema to check column
					schema, err := index.GetSchema(ctx)
					if err != nil {
						continue
					}

					// Check if the index contains this column (iterate over all columns)
					for _, col := range schema {
						if strings.EqualFold(col.Name, columnName) { // Case-insensitive comparison
							// Extract the value to search for
							if val, ok := node.Right.(*sqlparser.SQLVal); ok {
								value := string(val.Val)
								return index.GetName(), value, nil
							}
						}
					}
				}
			}
		}
	}

	return "", nil, nil
}

// ExecutePlan executes an optimized query plan
func (qo *QueryOptimizer) ExecutePlan(plan *QueryPlan, selectQuery *sqlparser.Select) ([]Row, error) {
	ctx := context.Background()
	table, err := qo.database.GetTable(ctx, plan.TableName)
	if err != nil {
		return nil, err
	}

	if !plan.UseIndex {
		// Fall back to full table scan
		return qo.executeFullTableScan(table, selectQuery, ctx)
	}

	// Use index-optimized query
	return qo.executeIndexQuery(table, plan, selectQuery, ctx)
}

// executeIndexQuery executes a query using an index
func (qo *QueryOptimizer) executeIndexQuery(table Table, plan *QueryPlan, selectQuery *sqlparser.Select, ctx context.Context) ([]Row, error) {
	// Get the index
	indexes, err := table.GetIndexes(ctx)
	if err != nil {
		return nil, err
	}

	var targetIndex Index
	for _, index := range indexes {
		if index.GetName() == plan.IndexName {
			targetIndex = index
			break
		}
	}

	if targetIndex == nil {
		return nil, fmt.Errorf("index %s not found", plan.IndexName)
	}

	// Use the index to find matching rowids
	indexEntries, err := targetIndex.SearchByKey(ctx, plan.IndexValue)
	if err != nil {
		// If index search fails, fall back to full table scan
		return qo.executeFullTableScan(table, selectQuery, ctx)
	}


	// If no entries found, return empty result
	if len(indexEntries) == 0 {
		return []Row{}, nil
	}

	// Extract rowids from index entries
	var matchingRows []Row
	for _, entry := range indexEntries {
		// Fetch the specific row by rowid (true index optimization!)
		row, err := table.GetRowByRowid(ctx, entry.Rowid)
		if err != nil {
			// Skip rows that can't be fetched, but continue processing
			continue
		}
		matchingRows = append(matchingRows, *row)
	}

	// Apply any additional filtering from the WHERE clause if needed
	if selectQuery.Where != nil {
		schema, err := table.GetSchema(ctx)
		if err != nil {
			return nil, err
		}

		var finalRows []Row
		for _, row := range matchingRows {
			match, err := evaluateWhereClause(selectQuery.Where.Expr, row, schema)
			if err != nil {
				continue // Skip rows with evaluation errors
			}
			if match {
				finalRows = append(finalRows, row)
			}
		}
		return finalRows, nil
	}

	return matchingRows, nil
}

// executeFullTableScan executes a query with full table scan (fallback)
func (qo *QueryOptimizer) executeFullTableScan(table Table, selectQuery *sqlparser.Select, ctx context.Context) ([]Row, error) {
	// Get all rows from the table
	allRows, err := table.GetRows(ctx)
	if err != nil {
		return nil, err
	}

	// Apply WHERE clause filtering if present
	var filteredRows []Row
	if selectQuery.Where != nil {
		schema, err := table.GetSchema(ctx)
		if err != nil {
			return nil, err
		}

		for _, row := range allRows {
			match, err := evaluateWhereClause(selectQuery.Where.Expr, row, schema)
			if err != nil {
				continue
			}
			if match {
				filteredRows = append(filteredRows, row)
			}
		}
	} else {
		filteredRows = allRows
	}

	// Apply LIMIT if specified
	if selectQuery.Limit != nil {
		if limit, ok := selectQuery.Limit.Rowcount.(*sqlparser.SQLVal); ok {
			if limitVal, err := parseIntValue(limit.Val); err == nil && limitVal >= 0 {
				if len(filteredRows) > limitVal {
					filteredRows = filteredRows[:limitVal]
				}
			}
		}
	}

	return filteredRows, nil
}

// evaluateWhereClause evaluates a WHERE clause condition for a row
func evaluateWhereClause(expr sqlparser.Expr, row Row, schema []Column) (bool, error) {
	switch node := expr.(type) {
	case *sqlparser.ComparisonExpr:
		return evaluateComparison(node, row, schema)
	case *sqlparser.AndExpr:
		left, err := evaluateWhereClause(node.Left, row, schema)
		if err != nil {
			return false, err
		}
		if !left {
			return false, nil // Short-circuit AND
		}
		return evaluateWhereClause(node.Right, row, schema)
	case *sqlparser.OrExpr:
		left, err := evaluateWhereClause(node.Left, row, schema)
		if err != nil {
			return false, err
		}
		if left {
			return true, nil // Short-circuit OR
		}
		return evaluateWhereClause(node.Right, row, schema)
	case *sqlparser.ParenExpr:
		return evaluateWhereClause(node.Expr, row, schema)
	default:
		return false, fmt.Errorf("unsupported WHERE expression type: %T", expr)
	}
}

// evaluateComparison evaluates a comparison expression
func evaluateComparison(comp *sqlparser.ComparisonExpr, row Row, schema []Column) (bool, error) {
	// Get column name and value
	colName, ok := comp.Left.(*sqlparser.ColName)
	if !ok {
		return false, fmt.Errorf("left side of comparison must be a column name")
	}

	// Find column index
	columnName := colName.Name.String()
	columnIndex := -1
	for _, col := range schema {
		if col.Name == columnName {
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
	compValue, err := extractComparisonValue(comp.Right)
	if err != nil {
		return false, fmt.Errorf("error extracting comparison value: %v", err)
	}

	// Perform comparison
	return compareValues(rowValue, compValue, comp.Operator)
}

// extractComparisonValue extracts a value from a comparison operand
func extractComparisonValue(expr sqlparser.Expr) (interface{}, error) {
	switch v := expr.(type) {
	case *sqlparser.SQLVal:
		switch v.Type {
		case sqlparser.StrVal:
			return string(v.Val), nil
		case sqlparser.IntVal:
			return string(v.Val), nil
		case sqlparser.FloatVal:
			return string(v.Val), nil
		default:
			return string(v.Val), nil
		}
	default:
		return nil, fmt.Errorf("unsupported comparison value type: %T", expr)
	}
}

// compareValues compares two values using the given operator
func compareValues(left, right interface{}, operator string) (bool, error) {
	// Convert both values to strings for comparison (simplified)
	leftStr := fmt.Sprintf("%v", left)
	rightStr := fmt.Sprintf("%v", right)

	switch operator {
	case "=":
		return leftStr == rightStr, nil
	case "!=", "<>":
		return leftStr != rightStr, nil
	case "<":
		return leftStr < rightStr, nil
	case ">":
		return leftStr > rightStr, nil
	case "<=":
		return leftStr <= rightStr, nil
	case ">=":
		return leftStr >= rightStr, nil
	default:
		return false, fmt.Errorf("unsupported comparison operator: %s", operator)
	}
}

// parseIntValue parses an integer value from bytes
func parseIntValue(val []byte) (int, error) {
	str := string(val)
	var result int
	_, err := fmt.Sscanf(str, "%d", &result)
	return result, err
}

// QueryPlan represents an execution plan for a query
type QueryPlan struct {
	QueryType  string
	TableName  string
	UseIndex   bool
	IndexName  string
	IndexValue interface{}
}
