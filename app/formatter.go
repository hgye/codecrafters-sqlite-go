package main

import (
	"fmt"
	"io"
	"strings"
)

// OutputFormatter handles different output formats
type OutputFormatter interface {
	FormatValue(value Value) string
	FormatRow(row *Row, schema []*Column) string
	FormatTable(rows []*Row, schema []*Column) string
	FormatCount(count int) string
}

// ConsoleFormatter formats output for console display
type ConsoleFormatter struct {
	io.Writer
}

// NewConsoleFormatter creates a new console formatter
func NewConsoleFormatter(writer io.Writer) *ConsoleFormatter {
	return &ConsoleFormatter{Writer: writer}
}

// FormatValue formats a single value
func (cf *ConsoleFormatter) FormatValue(value Value) string {
	if value == nil {
		return ""
	}
	return value.String()
}

// FormatRow formats a single row
func (cf *ConsoleFormatter) FormatRow(row *Row, schema []*Column) string {
	if row == nil {
		return ""
	}

	var parts []string
	for i, value := range row.Values {
		if i < len(schema) {
			parts = append(parts, cf.FormatValue(value))
		}
	}

	return strings.Join(parts, "\t")
}

// FormatTable formats multiple rows as a table
func (cf *ConsoleFormatter) FormatTable(rows []*Row, schema []*Column) string {
	if len(rows) == 0 {
		return ""
	}

	var result strings.Builder

	// Header
	var headers []string
	for _, col := range schema {
		headers = append(headers, col.Name)
	}
	result.WriteString(strings.Join(headers, "\t"))
	result.WriteString("\n")

	// Rows
	for _, row := range rows {
		result.WriteString(cf.FormatRow(row, schema))
		result.WriteString("\n")
	}

	return result.String()
}

// FormatCount formats a count result
func (cf *ConsoleFormatter) FormatCount(count int) string {
	return fmt.Sprintf("%d", count)
}

// JSONFormatter formats output as JSON (placeholder for future use)
type JSONFormatter struct {
	io.Writer
}

// NewJSONFormatter creates a new JSON formatter
func NewJSONFormatter(writer io.Writer) *JSONFormatter {
	return &JSONFormatter{Writer: writer}
}

// FormatValue formats a single value as JSON
func (jf *JSONFormatter) FormatValue(value Value) string {
	if value == nil {
		return "null"
	}

	switch value.Type() {
	case ValueTypeText:
		return fmt.Sprintf(`"%s"`, strings.ReplaceAll(value.String(), `"`, `\"`))
	case ValueTypeNull:
		return "null"
	default:
		return value.String()
	}
}

// FormatRow formats a single row as JSON
func (jf *JSONFormatter) FormatRow(row *Row, schema []*Column) string {
	if row == nil {
		return "{}"
	}

	var pairs []string
	for i, value := range row.Values {
		if i < len(schema) {
			key := fmt.Sprintf(`"%s"`, schema[i].Name)
			val := jf.FormatValue(value)
			pairs = append(pairs, fmt.Sprintf(`%s: %s`, key, val))
		}
	}

	return fmt.Sprintf("{%s}", strings.Join(pairs, ", "))
}

// FormatTable formats multiple rows as JSON array
func (jf *JSONFormatter) FormatTable(rows []*Row, schema []*Column) string {
	if len(rows) == 0 {
		return "[]"
	}

	var rowStrings []string
	for _, row := range rows {
		rowStrings = append(rowStrings, jf.FormatRow(row, schema))
	}

	return fmt.Sprintf("[%s]", strings.Join(rowStrings, ", "))
}

// FormatCount formats a count result as JSON
func (jf *JSONFormatter) FormatCount(count int) string {
	return fmt.Sprintf(`{"count": %d}`, count)
}
