package main

import (
	"fmt"
	"log"
	"os"
	"strings"
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

	engine, err := NewSqliteEngine(databaseFilePath)
	if err != nil {
		return err
	}
	defer engine.Close()

	// Extract command arguments
	var command string
	var sqlArgs string

	if len(args) > 2 && args[2][0] == '.' {
		command = args[2]
	} else {
		sqlArgs = strings.Join(args[2:], " ")
		command = "sql"
	}

	return engine.ExecuteCommand(command, sqlArgs)
}

// main function
func main() {
	err := runProgram(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
