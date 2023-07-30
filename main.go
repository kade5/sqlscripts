package main

import (
	"context"
	"database/sql"
	"log"
	"regexp"
	"sync"

	// "errors"
	"fmt"

	_ "github.com/microsoft/go-mssqldb"

	"os"
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

	count, err := readSPScripts()
	if err != nil {
		log.Fatal("Error with sql scripts: ", err.Error())
	}
	fmt.Printf("Read %d row(s) successfully.\n", count)
}

func ReadProp() (int, error) {
	ctx := context.Background()

	err := db.PingContext(ctx)
	if err != nil {
		return -1, err
	}

	tsql := fmt.Sprintf(`
		SELECT
			TOP 1
			SCHEMA_NAME(o.schema_id) AS [Schema],
			o.name,
			o.object_id,
			sm.definition
		FROM
			sys.objects o
		inner join sys.sql_modules sm on
			o.object_id = sm.object_id
		WHERE
			o.type = 'P';
		`)

	rows, err := db.QueryContext(ctx, tsql)
	if err != nil {
		return -1, err
	}

	defer rows.Close()

	var count int

	for rows.Next() {
		var definition, schema, name string
		var id int

		err := rows.Scan(&schema, &name, &id, &definition)
		if err != nil {
			return -1, err
		}

		definition = replaceCreateWithCreateOrAlter(definition)

		fmt.Printf("ID: %d Name: %s.%s\nDefinition: \n%s\n", id, schema, name, definition)
		count++
	}

	return count, nil
}

func readSPScripts() (int, error) {
	filepath := "data/sp_scripts/"
	ctx := context.Background()

	err := db.PingContext(ctx)
	if err != nil {
		log.Fatalf("Failed to establish connection")
		return -1, err
	}

	rows, err := db.Query(`
		SELECT
			SCHEMA_NAME(o.schema_id) AS [Schema],
			o.name,
			sm.definition
		FROM
			sys.objects o
		inner join sys.sql_modules sm on
			o.object_id = sm.object_id
		WHERE
			o.type = 'P';
		`)
	if err != nil {
		log.Fatalf("Failed to execute query")
		return -1, err
	}

	defer rows.Close()

	done := make(chan bool)
	var wg sync.WaitGroup

	for rows.Next() {
		var schema, name, definition string

		err := rows.Scan(&schema, &name, &definition)
		if err != nil {
			log.Fatalf("Error scanning row")
		}

		wg.Add(1)

		go func(schema, name, definition, filepath string) {
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

			_, err = file.WriteString(replaceCreateWithCreateOrAlter(definition))
			if err != nil {
				log.Fatalf("Failed to write to file")
				return
			}

			log.Printf("File %s created successfully", filename)
		}(schema, name, definition, filepath)
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

	return 0, nil
}
func replaceCreateWithCreateOrAlter(definition string) string {
	regexPattern := `(?i)CREATE\s+PROCEDURE`
	re := regexp.MustCompile(regexPattern)

	modifiedDefinition := re.ReplaceAllString(definition, "CREATE OR ALTER PROCEDURE")

	return modifiedDefinition
}
