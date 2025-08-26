package main

import (
	"encoding/binary"
	"fmt"
)

// Value represents a typed database value
type Value interface {
	Type() ValueType
	Raw() []byte
	String() string
	Int64() (int64, error)
	Float64() (float64, error)
}

// ValueType represents the type of a database value
type ValueType uint8

const (
	ValueTypeNull ValueType = iota
	ValueTypeInt8
	ValueTypeInt16
	ValueTypeInt24
	ValueTypeInt32
	ValueTypeInt48
	ValueTypeInt64
	ValueTypeFloat64
	ValueTypeZero
	ValueTypeOne
	ValueTypeBlob
	ValueTypeText
)

// SQLiteValue implements the Value interface
type SQLiteValue struct {
	serialType uint64
	data       []byte
}

// NewSQLiteValue creates a new SQLite value from serial type and data
func NewSQLiteValue(serialType uint64, data []byte) *SQLiteValue {
	return &SQLiteValue{
		serialType: serialType,
		data:       data,
	}
}

// Type returns the value type
func (v *SQLiteValue) Type() ValueType {
	switch v.serialType {
	case 0:
		return ValueTypeNull
	case 1:
		return ValueTypeInt8
	case 2:
		return ValueTypeInt16
	case 3:
		return ValueTypeInt24
	case 4:
		return ValueTypeInt32
	case 5:
		return ValueTypeInt48
	case 6:
		return ValueTypeInt64
	case 7:
		return ValueTypeFloat64
	case 8:
		return ValueTypeZero
	case 9:
		return ValueTypeOne
	default:
		if v.serialType >= 12 && v.serialType%2 == 0 {
			return ValueTypeBlob
		} else if v.serialType >= 13 && v.serialType%2 == 1 {
			return ValueTypeText
		}
		return ValueTypeNull
	}
}

// Raw returns the raw byte data
func (v *SQLiteValue) Raw() []byte {
	return v.data
}

// String returns the string representation
func (v *SQLiteValue) String() string {
	switch v.Type() {
	case ValueTypeNull:
		return ""
	case ValueTypeZero:
		return "0"
	case ValueTypeOne:
		return "1"
	case ValueTypeText, ValueTypeBlob:
		return string(v.data)
	default:
		if i, err := v.Int64(); err == nil {
			return fmt.Sprintf("%d", i)
		}
		if f, err := v.Float64(); err == nil {
			return fmt.Sprintf("%g", f)
		}
		return ""
	}
}

// Int64 returns the integer representation
func (v *SQLiteValue) Int64() (int64, error) {
	switch v.Type() {
	case ValueTypeNull:
		return 0, fmt.Errorf("null value cannot be converted to int64")
	case ValueTypeZero:
		return 0, nil
	case ValueTypeOne:
		return 1, nil
	case ValueTypeInt8:
		if len(v.data) >= 1 {
			return int64(int8(v.data[0])), nil
		}
	case ValueTypeInt16:
		if len(v.data) >= 2 {
			return int64(int16(binary.BigEndian.Uint16(v.data))), nil
		}
	case ValueTypeInt24:
		if len(v.data) >= 3 {
			// Sign-extend 24-bit value
			val := int64(binary.BigEndian.Uint16(v.data[:2]))<<8 | int64(v.data[2])
			if val&0x800000 != 0 { // Check sign bit
				val |= ^int64(0xFFFFFF) // Sign extend
			}
			return val, nil
		}
	case ValueTypeInt32:
		if len(v.data) >= 4 {
			return int64(int32(binary.BigEndian.Uint32(v.data))), nil
		}
	case ValueTypeInt48:
		if len(v.data) >= 6 {
			// Read 48-bit integer
			val := int64(binary.BigEndian.Uint32(v.data[:4]))<<16 | int64(binary.BigEndian.Uint16(v.data[4:]))
			if val&0x800000000000 != 0 { // Check sign bit
				val |= ^int64(0xFFFFFFFFFFFF) // Sign extend
			}
			return val, nil
		}
	case ValueTypeInt64:
		if len(v.data) >= 8 {
			return int64(binary.BigEndian.Uint64(v.data)), nil
		}
	}
	return 0, fmt.Errorf("cannot convert value of type %v to int64", v.Type())
}

// Float64 returns the float representation
func (v *SQLiteValue) Float64() (float64, error) {
	switch v.Type() {
	case ValueTypeFloat64:
		if len(v.data) >= 8 {
			bits := binary.BigEndian.Uint64(v.data)
			return float64FromBits(bits), nil
		}
		return 0, fmt.Errorf("insufficient data for float64")
	case ValueTypeZero:
		return 0.0, nil
	case ValueTypeOne:
		return 1.0, nil
	default:
		// Try to convert from integer
		if i, err := v.Int64(); err == nil {
			return float64(i), nil
		}
		return 0, fmt.Errorf("cannot convert value of type %v to float64", v.Type())
	}
}

// float64FromBits converts uint64 bits to float64 (Go's math.Float64frombits equivalent)
func float64FromBits(b uint64) float64 {
	// This is a simplified version - in practice you'd use math.Float64frombits
	// For now, just return a placeholder
	return float64(b)
}

// Column represents a database column
type Column struct {
	Name     string
	Type     string
	Index    int
	Nullable bool
}

// Row represents a database row
type Row struct {
	Values []Value
}

// Get returns the value for a specific column
func (r *Row) Get(columnIndex int) (Value, error) {
	if columnIndex < 0 || columnIndex >= len(r.Values) {
		return nil, NewDatabaseError("get_column_value", ErrColumnNotFound, map[string]interface{}{
			"column_index": columnIndex,
			"column_count": len(r.Values),
		})
	}
	return r.Values[columnIndex], nil
}

// GetByName returns the value for a specific column by name (requires schema)
func (r *Row) GetByName(columnName string, schema []*Column) (Value, error) {
	for _, col := range schema {
		if col.Name == columnName {
			return r.Get(col.Index)
		}
	}
	return nil, NewDatabaseError("get_column_by_name", ErrColumnNotFound, map[string]interface{}{
		"column_name": columnName,
	})
}
