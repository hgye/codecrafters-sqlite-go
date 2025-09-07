package main

import (
	"errors"
	"fmt"
)

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

// Error handling strategy constants
const (
	ErrorStrategyFail     = "fail"     // Return error immediately
	ErrorStrategySkip     = "skip"     // Skip and continue processing
	ErrorStrategyDefault  = "default"  // Use default value and continue
)

// ErrorHandler defines how to handle errors during processing
type ErrorHandler struct {
	Strategy string
	Logger   func(error) // Optional logger for skipped errors
}

// HandleProcessingError standardizes error handling during data processing
func (eh *ErrorHandler) HandleProcessingError(err error, context string) error {
	if err == nil {
		return nil
	}
	
	switch eh.Strategy {
	case ErrorStrategyFail:
		return fmt.Errorf("%s: %w", context, err)
	case ErrorStrategySkip:
		if eh.Logger != nil {
			eh.Logger(fmt.Errorf("skipped during %s: %w", context, err))
		}
		return nil // Continue processing
	case ErrorStrategyDefault:
		if eh.Logger != nil {
			eh.Logger(fmt.Errorf("using default during %s: %w", context, err))
		}
		return nil // Continue with default behavior
	default:
		return fmt.Errorf("%s: %w", context, err)
	}
}

// NewErrorHandler creates a new error handler with the specified strategy
func NewErrorHandler(strategy string, logger func(error)) *ErrorHandler {
	return &ErrorHandler{
		Strategy: strategy,
		Logger:   logger,
	}
}

// WrapDatabaseError wraps an error with database context
func WrapDatabaseError(operation string, err error, context map[string]interface{}) error {
	if err == nil {
		return nil
	}
	return NewDatabaseError(operation, err, context)
}

// IsRecoverableError checks if an error is recoverable (can be skipped)
func IsRecoverableError(err error) bool {
	if err == nil {
		return false
	}
	
	// These errors are typically recoverable in data processing contexts
	switch err {
	case ErrInvalidCellPointer, ErrInvalidVarint:
		return true
	}
	
	// Check wrapped errors
	var dbErr *DatabaseError
	if errors.As(err, &dbErr) {
		switch dbErr.Err {
		case ErrInvalidCellPointer, ErrInvalidVarint:
			return true
		}
	}
	
	return false
}
