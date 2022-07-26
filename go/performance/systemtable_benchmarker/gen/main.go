// Copyright 2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"math/rand"
	"os"
	"os/exec"
)

const (
	database = "system_table_benchmark"
	user     = "root"
	pass     = ""
	host     = "127.0.0.1"
	port     = "3307"

	numTables       = 10     // the number of tables we are going to model in this benchmark
	numRowsPerTable = 100000 // the number of rows per table
	numCommits      = 10

	insertDelta = int(numRowsPerTable * .25)
	deleteDelta = int(numRowsPerTable * .10)
	updateDelta = int(numRowsPerTable * .20)

	// TODO: Model schema as a memory table and generate shit accordingly

	schemaString      = "(pk int AUTO_INCREMENT PRIMARY KEY, c1 bigint DEFAULT 0, c2 char(1) DEFAULT NULL)"
	dummySchemaString = "(pk int PRIMARY KEY, c1 bigint DEFAULT 0, c2 char(1) DEFAULT NULL)"
)

// This is file created to generate a testing database used to evaluate the performance of system tables.
// TODO: Can also fork the mysql_random_data_loader and allow for package level random data generation
// TODO: Possible speed optimizations with AUTOCOMMIT
// TODO: Maybe schema as array
func main() {
	// 1. Initialize a connection with a dolt server and load in 100 tables with a predefined schema
	db, err := getDatabase()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	ctx := context.Background()

	tableNames, err := dropAndCreateTables(ctx, db, numTables)
	if err != nil {
		log.Fatal(err)
	}

	// 2. Generate random data in that table
	for _, tableName := range tableNames {
		err = runMySQLRandomDataLoad(database, tableName, numRowsPerTable)
		if err != nil {
			log.Fatal(err)
		}
	}

	// 3. Run the Initial Dolt Commit
	err = doltCommit(ctx, db, "Loaded the initial data")
	if err != nil {
		log.Fatal(err)
	}

	// 4. Run update/insert/delete algorithm
	for i := 0; i < numCommits; i++ {
		for _, table := range tableNames {
			err = simulateChangesToTable(ctx, db, table)
			if err != nil {
				log.Fatal(err)
			}
		}

		err = doltCommit(ctx, db, fmt.Sprintf("Commit %d", i))
		if err != nil {
			log.Fatal(err)
		}
	}

	os.Exit(0)
}

func getDatabase() (*sql.DB, error) {
	connStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, pass, host, port, database)
	db, err := sql.Open("mysql", connStr)

	if err != nil {
		return nil, err
	}

	return db, nil
}

func dropAndCreateTables(ctx context.Context, db *sql.DB, n int) ([]string, error) {
	tableNames := make([]string, 0)

	for i := 0; i < n; i++ {
		tableName := fmt.Sprintf("test%d", i)
		err := dropAndCreateTable(ctx, db, tableName)
		if err != nil {
			return nil, err
		}

		tableNames = append(tableNames, tableName)
	}

	return tableNames, nil
}

// dropAndCreateTable creates a table with a preset schema with the name tableName.
func dropAndCreateTable(ctx context.Context, db *sql.DB, tableName string) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}

	defer conn.Close()

	_, err = conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	if err != nil {
		return err
	}

	_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s %s;", tableName, schemaString))
	if err != nil {
		return err
	}

	return nil
}

func doltCommit(ctx context.Context, db *sql.DB, message string) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}

	defer conn.Close()

	_, err = conn.ExecContext(ctx, fmt.Sprintf(`CALL DOLT_COMMIT('-a', '-m', '%s')`, message))
	if err != nil {
		return err
	}

	return nil
}

// TODO: Make sure that utility is installing the file. Can also substitute with a http download each time
func runMySQLRandomDataLoad(database, table string, numRows int) error {
	args := []string{database, table,
		fmt.Sprintf("%d", numRows), fmt.Sprintf("--host=%s", host),
		fmt.Sprintf("--user=%s", user), fmt.Sprintf("--password=%s", pass),
		fmt.Sprintf("--port=%s", port),
	}

	cmd := exec.Command("./mysql_random_data_load", args...)
	var stdErr bytes.Buffer
	cmd.Stderr = &stdErr

	err := cmd.Run()
	if err != nil {
		log.Fatal(stdErr.String()) // For debugging purposes
		return err
	}

	return nil
}

// simulateChangesToTable randomly chooses to either insert rows, delete rows, or update existing rows of a relevant table
func simulateChangesToTable(ctx context.Context, db *sql.DB, tableName string) error {
	choice := rand.Intn(3)

	var err error
	switch choice {
	case 0:
		err = insertOperation(tableName)
	case 1:
		err = deleteOperation(ctx, db, tableName)
	case 2:
		err = updateOperation(ctx, db, tableName)
	}

	if err != nil {
		return err
	}

	return nil
}

// insertOperation can use the random data generator utility to run the update operation
func insertOperation(tableName string) error {
	err := runMySQLRandomDataLoad(database, tableName, insertDelta)
	if err != nil {
		return err
	}

	return nil
}

// deleteOperation sorts the table randomly and deletes the first numDelta rows
func deleteOperation(ctx context.Context, db *sql.DB, tableName string) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}

	defer conn.Close()

	_, err = conn.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s ORDER BY RAND() LIMIT %d", tableName, deleteDelta))
	if err != nil {
		return err
	}

	return nil
}

func updateOperation(ctx context.Context, db *sql.DB, tableName string) error {
	// Create a temporary table without the primary key
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}

	defer conn.Close()

	// TODO: Fix like bug
	_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE dummyTable%s", dummySchemaString))
	if err != nil {
		return err
	}

	// generate data for that temporary table
	err = runMySQLRandomDataLoad(database, "dummyTable", updateDelta)
	if err != nil {
		return err
	}

	// update the primary keys with the random distribution
	_, err = conn.ExecContext(ctx, "ALTER TABLE dummyTable ADD COLUMN pk2 int")
	if err != nil {
		return err
	}

	_, err = conn.ExecContext(ctx, fmt.Sprintf("update dummyTable, (select pk from %s order by RAND() LIMIT %d ) as pks set dummyTable.pk2 = pks.pk;", tableName, updateDelta))
	if err != nil {
		return err
	}

	// update the primary keys with the random distribution
	_, err = conn.ExecContext(ctx, "ALTER TABLE dummyTable drop primary key")
	if err != nil {
		return err
	}

	// update dummyTable, (select pk from test0 order by RAND() LIMIT 20000) as pks set dummyTable.pk = pks.pk;
	//_, err = conn.ExecContext(ctx, fmt.Sprintf("UPDATE dummyTable SET pk2 = (SELECT pk FROM %s ORDER BY RAND() LIMIT %d)", tableName, updateDelta))
	//if err != nil {
	//	return err
	//}
	//
	_, err = conn.ExecContext(ctx, "UPDATE IGNORE dummyTable SET pk = pk2")
	if err != nil {
		return err
	}

	// Simulate the update of a table with an UPDATE JOIN query
	_, err = conn.ExecContext(ctx, fmt.Sprintf("UPDATE dummyTable INNER JOIN %s ON %s.pk=dummyTable.pk SET %s.c1=dummyTable.c1", tableName, tableName, tableName))
	if err != nil {
		return err
	}

	_, err = conn.ExecContext(ctx, "DROP TABLE IF EXISTS dummyTable")
	if err != nil {
		return err
	}

	return nil
}
