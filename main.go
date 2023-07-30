package main

import (
	"context"
	"database/sql"
	"log"

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

	count, err := ReadProp()
	if err != nil {
		log.Fatal("Error Reading Properties: ", err.Error())
	}
	fmt.Printf("Read %d row(s) successfully.\n", count)
}

func ReadProp() (int, error) {
	ctx := context.Background()

	err := db.PingContext(ctx)
	if err != nil {
		return -1, err
	}

	tsql := fmt.Sprintf("SELECT [definition] FROM sys.sql_modules where object_id = (OBJECT_ID(N'bi.vw_SFFields'));")

	rows, err := db.QueryContext(ctx, tsql)
	if err != nil {
		return -1, err
	}

	defer rows.Close()

	var count int

	for rows.Next() {
		var definition string
		// var id int

		err := rows.Scan(&definition)
		if err != nil {
			return -1, err
		}

		fmt.Printf("Definition: \n%s\n", definition)
		count++
	}

	return count, nil
}
