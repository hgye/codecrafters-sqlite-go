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

	// Process each expression in the SELECT clause
	for _, expr := range selectExprs {
		fmt.Fprintf(os.Stderr, "Processing expression of type: %T\n", expr)
		switch selectExpr := expr.(type) {
		case *sqlparser.StarExpr:
			// Handle SELECT *
			fmt.Fprintf(os.Stderr, "Found SELECT * - need to return all columns\n")
			// TODO: Implement returning all columns

		case *sqlparser.AliasedExpr:
			// Check if it's a function call like COUNT(*) or a regular column
			fmt.Fprintf(os.Stderr, "Processing AliasedExpr with inner type: %T\n", selectExpr.Expr)
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

				// Call handleSelectColumn to validate and get column info
				// fmt.Fprintf(os.Stderr, "Calling handleSelectColumn for table: %s, column: %s\n", tableName, columnName)
				err := handleSelectColumn(tableName, columnName, db)
				if err != nil {
					// fmt.Fprintf(os.Stderr, "Error in handleSelectColumn: %v\n", err)
					return err
				}
				// fmt.Fprintf(os.Stderr, "handleSelectColumn completed successfully\n")

			default:
				fmt.Fprintf(os.Stderr, "Unknown expression type in AliasedExpr: %T\n", innerExpr)
			}

		default:
			fmt.Fprintf(os.Stderr, "Unknown expression type in SELECT: %T\n", selectExpr)
		}
	}

	return nil
}

// handleCountFunction processes COUNT(*) functions
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

// handleSelectColumn
func handleSelectColumn(tableName string, colName string, db *SQLiteDB) error {
	table := db.GetTable(tableName)
	if table == nil {
		return fmt.Errorf("table %s not found", tableName)
	}

	fmt.Fprintf(os.Stderr, "Schema SQL: %s\n", table.SchemaSQL)

	// Normalize SQLite syntax to MySQL syntax for sqlparser
	normalizedSQL := normalizeSQLiteToMySQL(table.SchemaSQL)
	fmt.Fprintf(os.Stderr, "Normalized SQL: %s\n", normalizedSQL)

	// Try to parse with sqlparser
	stmt, err := sqlparser.Parse(normalizedSQL)
	if err != nil {
		return fmt.Errorf("sqlparser failed even after normalization: %v", err)
	}

	switch parsedStmt := stmt.(type) {
	case *sqlparser.DDL:
		if parsedStmt.Action != "create" || parsedStmt.TableSpec == nil {
			return fmt.Errorf("unexpected DDL statement: action=%s", parsedStmt.Action)
		}

		fmt.Fprintf(os.Stderr, "Found CREATE TABLE statement (via sqlparser) for table: %s\n", tableName)
		fmt.Fprintf(os.Stderr, "Columns in table:\n")

		// Look through the columns in the TableSpec to find the column index
		columnIndex := -1
		for i, col := range parsedStmt.TableSpec.Columns {
			columnName := col.Name.String()
			columnType := col.Type.Type
			fmt.Fprintf(os.Stderr, "  - %s (%s)\n", columnName, columnType)

			if strings.EqualFold(columnName, colName) {
				columnIndex = i
				fmt.Fprintf(os.Stderr, "Found target column %s at index %d\n", columnName, i)
				break
			}
		}

		if columnIndex == -1 {
			return fmt.Errorf("column %s not found in table %s", colName, tableName)
		}

		// Now retrieve actual data from the table using proper table B-tree reading
		return readTableData(table, columnIndex, db)

	default:
		return fmt.Errorf("unsupported schema SQL statement type: %T", parsedStmt)
	}
}

// retrieveColumnData retrieves and prints data from a specific column in a table
func readTableData(table *Table, columnIndex int, db *SQLiteDB) error {
	// Calculate the page offset using (rootpage-1) * pagesize
	pageOffset := int64((table.RootPage - 1)) * int64(db.header.PageSize)
	fmt.Fprintf(os.Stderr, "Table %s: RootPage=%d, PageSize=%d, Offset=%d\n",
		table.Name, table.RootPage, db.header.PageSize, pageOffset)

	// Seek to the table's root page
	_, err := db.file.Seek(pageOffset, 0)
	if err != nil {
		return fmt.Errorf("failed to seek to table page: %w", err)
	}

	// Read the page header
	pageHeader, err := db.readPageHeader()
	if err != nil {
		return fmt.Errorf("failed to read page header: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Page type: 0x%02X, CellCount: %d, CellContentStart: %d\n",
		pageHeader.PageType, pageHeader.CellCount, pageHeader.CellContentStart)

	// Check if this is a table B-tree leaf page (type 0x0d)
	if pageHeader.PageType != 0x0d {
		return fmt.Errorf("expected table B-tree leaf page (0x0d), got 0x%02X", pageHeader.PageType)
	}

	// Read cell pointers (these are relative offsets within the page)
	cellPointers, err := db.readCellPointerArray(pageHeader.CellCount)
	if err != nil {
		return fmt.Errorf("failed to read cell pointers: %w", err)
	}

	// Read all cells and extract the specified column data
	for i, cellPointer := range cellPointers {
		// Calculate absolute offset: page start + cell pointer
		cellAbsoluteOffset := pageOffset + int64(cellPointer)
		fmt.Fprintf(os.Stderr, "Cell %d: pointer=0x%04X (%d), absolute offset=%d\n",
			i, uint16(cellPointer), uint16(cellPointer), cellAbsoluteOffset)

		// Seek to the absolute cell position
		_, err := db.file.Seek(cellAbsoluteOffset, 0)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error seeking to cell %d: %v\n", i, err)
			continue
		}

		// Read table B-tree leaf cell manually (different format than schema cells)
		cell, err := readTableCell(db.file)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading table cell %d: %v\n", i, err)
			continue
		}

		fmt.Fprintf(os.Stderr, "Cell %d: PayloadSize=%d, Rowid=%d, Values=%d\n",
			i, cell.PayloadSize, cell.Rowid, len(cell.Record.RecordBody.Values))

		// Extract the value from the specified column
		if len(cell.Record.RecordBody.Values) > columnIndex {
			value := cell.Record.RecordBody.Values[columnIndex]
			if value != nil {
				// Print the actual column value
				switch v := value.(type) {
				case []byte:
					fmt.Println(string(v))
				case string:
					fmt.Println(v)
				case int64:
					fmt.Println(v)
				case float64:
					fmt.Println(v)
				default:
					fmt.Printf("%v\n", v)
				}
			}
		} else {
			fmt.Fprintf(os.Stderr, "Cell %d: not enough values (has %d, need %d)\n",
				i, len(cell.Record.RecordBody.Values), columnIndex+1)
		}
	}

	return nil
}

// readTableCell reads a table B-tree leaf cell (type 0x0d) from the current file position
func readTableCell(file *os.File) (*Cell, error) {
	var cell Cell

	// Read payload size and rowid varints from file
	payloadData := make([]byte, 64) // Read enough bytes to parse varints
	if _, err := file.Read(payloadData); err != nil {
		return nil, err
	}

	var bytesRead int
	cell.PayloadSize, bytesRead = readVarint(payloadData, 0)
	var rowidBytesRead int
	cell.Rowid, rowidBytesRead = readVarint(payloadData, bytesRead)
	totalVarintBytes := bytesRead + rowidBytesRead

	// Seek back to the start of payload data (after varints)
	currentPos, _ := file.Seek(0, 1) // Get current position
	payloadStart := currentPos - int64(len(payloadData)) + int64(totalVarintBytes)
	if _, err := file.Seek(payloadStart, 0); err != nil {
		return nil, err
	}

	// Read the actual payload data
	payloadSize := int(cell.PayloadSize)
	payload := make([]byte, payloadSize)
	if _, err := file.Read(payload); err != nil {
		return nil, err
	}

	// Parse record from payload
	var record Record
	var headerOffset int
	record.RecordHeader, headerOffset = readRecordHeader(payload, 0)
	record.RecordBody, _ = readRecordBody(payload, headerOffset, record.RecordHeader)
	cell.Record = record

	return &cell, nil
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

// normalizeSQLiteToMySQL converts SQLite-specific syntax to MySQL syntax for sqlparser
func normalizeSQLiteToMySQL(sql string) string {
	// Fix MySQL syntax: "primary key autoincrement" should be "AUTO_INCREMENT PRIMARY KEY"
	// or just "AUTO_INCREMENT" (as AUTO_INCREMENT implies PRIMARY KEY in MySQL)
	normalized := strings.ReplaceAll(sql, "primary key autoincrement", "AUTO_INCREMENT PRIMARY KEY")
	normalized = strings.ReplaceAll(normalized, "PRIMARY KEY AUTOINCREMENT", "AUTO_INCREMENT PRIMARY KEY")

	return normalized
}
