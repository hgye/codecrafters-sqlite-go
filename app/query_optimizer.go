package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

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
		// No index optimization - return error instead of fallback
		return nil, fmt.Errorf("no index optimization available for query")
	}

	// Use index-optimized query with timing
	return qo.executeIndexQueryWithTiming(table, plan, selectQuery, ctx)
}

// executeIndexQueryWithTiming executes a query using an index with detailed timing
func (qo *QueryOptimizer) executeIndexQueryWithTiming(table Table, plan *QueryPlan, selectQuery *sqlparser.Select, ctx context.Context) ([]Row, error) {
	// 	overallStart := time.Now()
	// fmt.Printf("[TIMING] Starting index query execution\n")

	// Get the index with timing
	// 	indexLookupStart := time.Now()
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

	// 	indexLookupDuration := time.Since(indexLookupStart)
	// fmt.Printf("[TIMING] Index lookup took: %v\n", indexLookupDuration)

	// Use the index to find matching rowids with timing
	// 	indexSearchStart := time.Now()
	// fmt.Printf("[TIMING] Starting index search for key: %v\n", plan.IndexValue)
	indexEntries, err := targetIndex.SearchByKey(ctx, plan.IndexValue)
	// 	indexSearchDuration := time.Since(indexSearchStart)
	// fmt.Printf("[TIMING] Index search took: %v (found %d entries)\n", indexSearchDuration, len(indexEntries))
	if err != nil {
		// No fallback - return the error
		return nil, fmt.Errorf("index search failed: %w", err)
	}

	// If no entries found, return empty result
	if len(indexEntries) == 0 {
		return []Row{}, nil
	}

	// Extract rowids from index entries using parallel fetching with timing
	// 	rowFetchStart := time.Now()
	// fmt.Printf("[TIMING] Starting parallel row fetching for %d entries\n", len(indexEntries))
	matchingRows := qo.fetchRowsParallel(ctx, table, indexEntries)
	// 	rowFetchDuration := time.Since(rowFetchStart)
	// fmt.Printf("[TIMING] Parallel row fetching took: %v (fetched %d rows)\n", rowFetchDuration, len(matchingRows))

	// Apply any additional filtering from the WHERE clause if needed with timing
	if selectQuery.Where != nil {
		// 		filterStart := time.Now()
		// fmt.Printf("[TIMING] Starting WHERE clause filtering\n")
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
		// 		filterDuration := time.Since(filterStart)
		// fmt.Printf("[TIMING] WHERE clause filtering took: %v (filtered to %d rows)\n", filterDuration, len(finalRows))
		// 		overallDuration := time.Since(overallStart)
		// fmt.Printf("[TIMING] Overall query execution took: %v\n", overallDuration)
		return finalRows, nil
	}

	// 	overallDuration := time.Since(overallStart)
	// fmt.Printf("[TIMING] Overall query execution took: %v\n", overallDuration)
	return matchingRows, nil
}

// // executeFullTableScan executes a query with full table scan (fallback)
// func (qo *QueryOptimizer) executeFullTableScan(table Table, selectQuery *sqlparser.Select, ctx context.Context) ([]Row, error) {
// 	// Get all rows from the table
// 	allRows, err := table.GetRows(ctx)
// 	if err != nil {
// 		return nil, err
// 	}

// 	// Apply WHERE clause filtering if present
// 	var filteredRows []Row
// 	if selectQuery.Where != nil {
// 		schema, err := table.GetSchema(ctx)
// 		if err != nil {
// 			return nil, err
// 		}

// 		for _, row := range allRows {
// 			match, err := evaluateWhereClause(selectQuery.Where.Expr, row, schema)
// 			if err != nil {
// 				continue
// 			}
// 			if match {
// 				filteredRows = append(filteredRows, row)
// 			}
// 		}
// 	} else {
// 		filteredRows = allRows
// 	}

// 	// Apply LIMIT if specified
// 	if selectQuery.Limit != nil {
// 		if limit, ok := selectQuery.Limit.Rowcount.(*sqlparser.SQLVal); ok {
// 			if limitVal, err := parseIntValue(limit.Val); err == nil && limitVal >= 0 {
// 				if len(filteredRows) > limitVal {
// 					filteredRows = filteredRows[:limitVal]
// 				}
// 			}
// 		}
// 	}

// 	return filteredRows, nil
// }

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
// func parseIntValue(val []byte) (int, error) {
// 	str := string(val)
// 	var result int
// 	_, err := fmt.Sscanf(str, "%d", &result)
// 	return result, err
// }

// fetchRowsParallel fetches rows by rowid in parallel using goroutines
func (qo *QueryOptimizer) fetchRowsParallel(ctx context.Context, table Table, indexEntries []IndexEntry) []Row {
	// 	setupStart := time.Now()
	// Create a context with 3-second timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	// Use a worker pool pattern with limited concurrency
	maxWorkers := 10 // Limit concurrent fetches to avoid overwhelming the system
	if len(indexEntries) < maxWorkers {
		maxWorkers = len(indexEntries)
	}
	// 	setupDuration := time.Since(setupStart)
	// fmt.Printf("[TIMING] Worker setup took: %v (using %d workers for %d entries)\\n", setupDuration, maxWorkers, len(indexEntries))

	type rowResult struct {
		row   *Row
		rowid int64
		err   error
		index int
	}

	// Channel for work items and results
	workChan := make(chan struct {
		entry IndexEntry
		index int
	}, len(indexEntries))
	resultChan := make(chan rowResult, len(indexEntries))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for work := range workChan {
				// Check for timeout before starting work
				select {
				case <-timeoutCtx.Done():
					return
				default:
				}

				row, err := table.GetRowByRowid(timeoutCtx, work.entry.Rowid)

				resultChan <- rowResult{
					row:   row,
					rowid: work.entry.Rowid,
					err:   err,
					index: work.index,
				}
			}
		}(i)
	}

	// Send work to workers
	// 	workDistributionStart := time.Now()
	for i, entry := range indexEntries {
		workChan <- struct {
			entry IndexEntry
			index int
		}{entry: entry, index: i}
	}
	close(workChan)
	// 	workDistributionDuration := time.Since(workDistributionStart)
	// fmt.Printf("[TIMING] Work distribution took: %v\\n", workDistributionDuration)

	// Wait for all workers to finish
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results maintaining order
	// 	resultCollectionStart := time.Now()
	results := make([]*Row, len(indexEntries))
	validCount := 0
	for result := range resultChan {
		if result.err == nil && result.row != nil {
			results[result.index] = result.row
			validCount++
		}
	}
	// 	resultCollectionDuration := time.Since(resultCollectionStart)
	// fmt.Printf("[TIMING] Result collection took: %v (collected %d valid results)\\n", resultCollectionDuration, validCount)

	// Filter out nil entries and preserve order
	matchingRows := make([]Row, 0, validCount)
	for _, row := range results {
		if row != nil {
			matchingRows = append(matchingRows, *row)
		}
	}

	return matchingRows
}

// QueryPlan represents an execution plan for a query
type QueryPlan struct {
	QueryType  string
	TableName  string
	UseIndex   bool
	IndexName  string
	IndexValue interface{}
}
