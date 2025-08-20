package main

import "fmt"

// Domain-specific errors
var (
	ErrInvalidDatabase    = fmt.Errorf("invalid database file")
	ErrTableNotFound      = fmt.Errorf("table not found")
	ErrColumnNotFound     = fmt.Errorf("column not found")
	ErrInvalidPageType    = fmt.Errorf("invalid page type")
	ErrInsufficientData   = fmt.Errorf("insufficient data")
	ErrInvalidCellPointer = fmt.Errorf("invalid cell pointer")
	ErrInvalidVarint      = fmt.Errorf("invalid varint")
)

// DatabaseError represents a database-specific error
type DatabaseError struct {
	Operation string
	Err       error
	Context   map[string]interface{}
}

func (e *DatabaseError) Error() string {
	if e.Context == nil {
		return fmt.Sprintf("database error in %s: %v", e.Operation, e.Err)
	}
	return fmt.Sprintf("database error in %s: %v (context: %+v)", e.Operation, e.Err, e.Context)
}

func (e *DatabaseError) Unwrap() error {
	return e.Err
}

// NewDatabaseError creates a new database error
func NewDatabaseError(operation string, err error, context map[string]interface{}) *DatabaseError {
	return &DatabaseError{
		Operation: operation,
		Err:       err,
		Context:   context,
	}
}
