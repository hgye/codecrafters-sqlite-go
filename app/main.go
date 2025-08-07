package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

type DatabaseHeader struct {
	MagicNumber     [16]byte
	PageSize        uint16
	FileFormatWrite uint8
	FileFormatRead  uint8
	ReservedBytes   uint8
	MaxPayload      uint8
	MinPayload      uint8
	LeafPayload     uint8
	FileChangeCount uint32
	DatabaseSize    uint32
	FirstFreePage   uint32
	FreePageCount   uint32
	SchemaCookie    uint32
	SchemaFormat    uint32
	DefaultCache    uint32
	LargestBTree    uint32
	TextEncoding    uint32
	UserVersion     uint32
	IncrVacuum      uint32
	AppID           uint32
	Reserved        [20]byte
	VersionValid    uint32
	SQLiteVersion   uint32
}

type PageHeader struct {
	PageType         uint8
	FirstFreeblock   uint16
	CellCount        uint16
	CellContentStart uint16
	FragmentedBytes  uint8
	// RightmostPointer uint32 // Only for interior pages
}

type CellPointer uint16

// Cell represents a B-tree cell (varies by page type)
type Cell struct {
	// For Table B-Tree Leaf Cell (header 0x0d):
	PayloadSize uint64 // varint: total bytes of payload including overflow
	Rowid       uint64 // varint: integer key (rowid)
	Record      Record // parsed record from payload
	// OverflowPage uint32 // 4-byte page number for overflow (if needed)
}

type Record struct {
	RecordHeader
	RecordBody
}

// RecordHeader represents the header of a record in the payload
type RecordHeader struct {
	HeaderSize  uint64  // varint: total bytes in header including this varint
	SerialTypes []uint8 // serial types: one per column, determines datatype
}

// RecordBody represents the body/data portion of a record
type RecordBody struct {
	Values []interface{} // actual column values based on serial types
}

// SerialType constants for SQLite record format
const (
	SerialTypeNull    = 0
	SerialTypeInt8    = 1
	SerialTypeInt16   = 2
	SerialTypeInt24   = 3
	SerialTypeInt32   = 4
	SerialTypeInt48   = 5
	SerialTypeInt64   = 6
	SerialTypeFloat64 = 7
	SerialTypeZero    = 8 // schema format 4+
	SerialTypeOne     = 9 // schema format 4+
	// SerialType >= 12 and even: BLOB with (N-12)/2 bytes
	// SerialType >= 13 and odd: TEXT with (N-13)/2 bytes
)

// readVarint reads a variable-length integer from the data
func readVarint(data []byte, offset int) (value uint64, bytesRead int) {
	var result uint64
	for i := 0; i < 9 && offset+i < len(data); i++ {
		b := data[offset+i]
		if i == 8 {
			// 9th byte uses all 8 bits
			result = (result << 8) | uint64(b)
			return result, i + 1
		}
		// First 8 bytes use lower 7 bits
		result = (result << 7) | uint64(b&0x7F)
		if (b & 0x80) == 0 {
			// High bit clear means this is the last byte
			return result, i + 1
		}
	}
	return result, 0 // Invalid varint
}

// getSerialTypeSize returns the size in bytes for a given serial type
func getSerialTypeSize(serialType uint8) int {
	switch serialType {
	case 0, 8, 9:
		return 0
	case 1:
		return 1
	case 2:
		return 2
	case 3:
		return 3
	case 4:
		return 4
	case 5:
		return 6
	case 6, 7:
		return 8
	default:
		if serialType >= 12 && serialType%2 == 0 {
			// BLOB: (N-12)/2 bytes
			return int((serialType - 12) / 2)
		} else if serialType >= 13 && serialType%2 == 1 {
			// TEXT: (N-13)/2 bytes
			return int((serialType - 13) / 2)
		}
		return 0
	}
}

func readDatabaseHeader(file *os.File) (*DatabaseHeader, error) {
	var dbHeader DatabaseHeader
	if err := binary.Read(file, binary.BigEndian, &dbHeader); err != nil {
		return nil, err
	}
	return &dbHeader, nil
}

func readPageHeader(file *os.File) (*PageHeader, error) {
	var pageHeader PageHeader
	if err := binary.Read(file, binary.BigEndian, &pageHeader); err != nil {
		return nil, err
	}
	return &pageHeader, nil
}

func readCellPointerArray(file *os.File, cellCount uint16) ([]CellPointer, error) {
	cellPointers := make([]uint16, cellCount)
	if err := binary.Read(file, binary.BigEndian, &cellPointers); err != nil {
		return nil, err
	}

	// Convert []uint16 to []CellPointer
	result := make([]CellPointer, cellCount)
	for i, pointer := range cellPointers {
		result[i] = CellPointer(pointer)
	}
	return result, nil
}

func (cp CellPointer) Offset() uint16 {
	return uint16(cp)
}

func (cp CellPointer) IsValid() bool {
	return cp > 0
}

func readCell(file *os.File, cellPointer CellPointer) (*Cell, error) {
	offset := cellPointer.Offset()
	if _, err := file.Seek(int64(offset), 0); err != nil {
		return nil, err
	}

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

	// Read the actual payload data
	payloadSize := int(cell.PayloadSize)
	payload := make([]byte, payloadSize)
	if _, err := file.Seek(int64(cellPointer.Offset())+int64(totalVarintBytes), 0); err != nil {
		return nil, err
	}
	if _, err := file.Read(payload); err != nil {
		return nil, err
	}

	// Parse record from payload (always one record per cell in table b-tree leaf)
	var record Record
	var headerOffset int
	record.RecordHeader, headerOffset = readRecordHeader(payload, 0)
	record.RecordBody, _ = readRecordBody(payload, headerOffset, record.RecordHeader)
	cell.Record = record

	return &cell, nil
}

func readRecordHeader(data []byte, offset int) (RecordHeader, int) {
	var header RecordHeader
	var bytesRead int
	header.HeaderSize, bytesRead = readVarint(data, offset)
	offset += bytesRead

	if header.HeaderSize == 0 {
		return header, offset // No header
	}

	// Calculate how many serial types we need to read
	headerEnd := int(header.HeaderSize)
	for offset < headerEnd {
		var serialType uint64
		serialType, bytesRead = readVarint(data, offset)
		header.SerialTypes = append(header.SerialTypes, uint8(serialType))
		offset += bytesRead
	}

	return header, offset
}

func readRecordBody(data []byte, offset int, header RecordHeader) (RecordBody, int) {
	var body RecordBody
	body.Values = make([]interface{}, len(header.SerialTypes))

	for i, serialType := range header.SerialTypes {
		size := getSerialTypeSize(serialType)
		if size == 0 {
			body.Values[i] = nil // NULL value
			continue
		}
		if offset+size > len(data) {
			log.Fatal("Not enough data for record body")
		}
		value := data[offset : offset+size]
		body.Values[i] = value // Store raw bytes for now
		offset += size
	}
	return body, offset
}

// Usage: your_program.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	switch command {
	case ".dbinfo":
		databaseFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}

		// Read database header
		dbHeader, err := readDatabaseHeader(databaseFile)
		if err != nil {
			log.Fatal(err)
		}
		// You can use print statements as follows for debugging, they'll be visible when running tests.
		fmt.Fprintln(os.Stderr, "Logs from your program will appear here!")

		// Uncomment this to pass the first stage
		fmt.Printf("database page size: %v\n", dbHeader.PageSize)

		// Read the first page header
		pageHeader, err := readPageHeader(databaseFile)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("number of tables: %v\n", pageHeader.CellCount)

		cellPointerArray, err := readCellPointerArray(databaseFile, pageHeader.CellCount)
		if err != nil {
			log.Fatal(err)
		}

		// every cell is a record here
		// Read first cell for sqlite_master table
		if len(cellPointerArray) == 0 {
			fmt.Println("No cell pointers found")
			os.Exit(1)
		}

		// cells := make([]*Cell, len(cellPointerArray))
		for _, pointer := range cellPointerArray {
			cell, err := readCell(databaseFile, pointer)
			if err != nil {
				log.Fatal(err)
			}
			// cells = append(cells, cell)
			fmt.Printf("%s ", string(cell.Record.Values[2].([]byte))) // Assuming 3rd column is table name
		}
		fmt.Println()

	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
