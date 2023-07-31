package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"regexp"
	"sync"

	_ "github.com/microsoft/go-mssqldb"
)

var db *sql.DB

func main() {
	connString := os.Args[1]
	fmt.Println(connString)
	var err error
	db, err = sql.Open("sqlserver", connString)

	if err != nil {
		log.Fatal("Error creating connection pool: ", err.Error())
	}
	ctx := context.Background()
	err = db.PingContext(ctx)
	if err != nil {
		log.Fatal(err.Error())
	}
	fmt.Printf("Connected!\n")

	count, err := buildScripts("PROCEDURE")
	if err != nil {
		log.Fatal("Error with sp sql scripts: ", err.Error())
	}
	fmt.Printf("Succesfully wrote %d stored procedures to file(s)\n", count)

	fun_count, err := buildScripts("FUNCTION")
	if err != nil {
		log.Fatal("Error with function sql scripts: ", err.Error())
	}
	fmt.Printf("Succesfully wrote %d functions to file(s)\n", fun_count)

	view_count, err := buildScripts("VIEW")
	if err != nil {
		log.Fatal("Error with view sql scripts: ", err.Error())
	}
	fmt.Printf("Succesfully wrote %d views to file(s)\n", view_count)
}

func buildScripts(objectName string) (int, error) {
	var filepath string
	switch objectName {
	case "PROCEDURE":
		filepath = "sp_scripts/"
	case "FUNCTION":
		filepath = "function_scripts/"
	case "VIEW":
		filepath = "view_scripts/"
	default:
		return -1, errors.New("Invalid object name")
	}
	
	ctx := context.Background()

	err := db.PingContext(ctx)
	if err != nil {
		log.Fatalf("Failed to establish connection")
		return -1, err
	}

	rows, err := db.Query(getQuery(objectName))
	if err != nil {
		log.Fatalf("Failed to execute query: %v", err)
		return -1, err
	}

	defer rows.Close()

	done := make(chan bool)
	var wg sync.WaitGroup

	count := 0

	for rows.Next() {
		var schema, name, definition string

		err := rows.Scan(&schema, &name, &definition)
		if err != nil {
			log.Fatalf("Error scanning row")
		}

		wg.Add(1)

		go func(schema, name, definition, filepath, objectName string) {
			defer wg.Done()
			var filename string
			if schema == "dbo" {
				filename = fmt.Sprintf("%s.sql", name)
			} else {
				filename = fmt.Sprintf("%s.%s.sql", schema, name)
			}

			file, err := os.Create(filepath + filename)
			if err != nil {
				log.Fatalf("Failed to create file")
				return
			}
			defer file.Close()

			_, err = file.WriteString(replaceCreateWithCreateOrAlter(definition, objectName))
			if err != nil {
				log.Fatalf("Failed to write to file")
				return
			}

			log.Printf("File %s created successfully", filename)
		}(schema, name, definition, filepath, objectName)
		count++
	}

	go func() {
		wg.Wait()
		done <- true
	}()

	<-done
	if err := rows.Err(); err != nil {
		log.Fatalf("Error processing rows: %v", err)
		return -1, err
	}

	return count, nil
}

func getQuery(objectName string) string {
	switch objectName {
	case "PROCEDURE":
		return `
		SELECT
			SCHEMA_NAME(o.schema_id) AS [Schema],
			o.name,
			sm.definition
		FROM
			sys.objects o
		inner join sys.sql_modules sm on
			o.object_id = sm.object_id
		WHERE
			o.type in ('P');
		`
	case "FUNCTION":
		return `
		SELECT
			SCHEMA_NAME(o.schema_id) AS [Schema],
			o.name,
			sm.definition
		FROM
			sys.objects o
		inner join sys.sql_modules sm on
			o.object_id = sm.object_id
		WHERE
			o.type in ('FN', 'FS', 'FT', 'IF');

		`
	case "VIEW":
		return `
		SELECT
			SCHEMA_NAME(o.schema_id) AS [Schema],
			o.name,
			sm.definition
		FROM
			sys.objects o
		inner join sys.sql_modules sm on
			o.object_id = sm.object_id
		WHERE
			o.type in ('V');
		`
	default:
		return ""
	}
}

func replaceCreateWithCreateOrAlter(definition, objectName string) string {
	regexPattern := fmt.Sprintf(`(?i)CREATE\s+%s`, objectName)
	re := regexp.MustCompile(regexPattern)

	modifiedDefinition := re.ReplaceAllString(definition, fmt.Sprintf("CREATE OR ALTER %s", objectName))

	return modifiedDefinition
}
