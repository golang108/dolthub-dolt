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

package import_benchmarker

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/performance/utils/sysbench_runner"
)

const (
	defaultHost = "127.0.0.1"
	defaultPort = 3306

	defaultSocket   = "/var/run/mysqld/mysqld.sock"
	defaultProtocol = "unix"
	dbName          = "test"
)

func BenchmarkMySQLImportJobs(jobs []*ImportBenchmarkJob, workingDir string) []result {
	ctx := context.Background()
	withCancelCtx, cancel := context.WithCancel(ctx)

	gServer, serverCtx := errgroup.WithContext(withCancelCtx)
	var serverErr bytes.Buffer

	// Assume first server is okay
	server := getMysqlServer(serverCtx, jobs[0].ExecPath, getServersArgs())
	server.Stderr = &serverErr

	// launch the mysql server
	gServer.Go(func() error {
		err := server.Run()
		if err != nil {
			log.Fatal(serverErr.String())
			return err
		}

		return nil
	})

	// sleep to allow the server to start
	time.Sleep(5 * time.Second)

	// setup the relevant testing database and permissions
	// TODO: Switch to supporting connection protocol of unix and so forth
	mConfig := sysbench_runner.MysqlConfig{Socket: defaultSocket, ConnectionProtocol: "tcp", Port: defaultPort, Host: defaultHost}
	err := sysbench_runner.SetupDB(ctx, mConfig, dbName)

	if err != nil {
		cancel()
		log.Fatal(err.Error())
	}

	// handle user interrupt
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		<-quit
		defer wg.Done()
		signal.Stop(quit)
		cancel()
	}()

	results := make([]result, len(jobs))

	for i, job := range jobs {
		// run the actual test
		br := testing.Benchmark(func(b *testing.B) {
			benchmarkLoadData(ctx, b, mConfig, job)
		})

		results[i] = result{
			name:        job.Name,
			format:      job.Format,
			rows:        job.NumRows,
			columns:     len(genSampleCols()),
			sizeOnDisk:  0, // TODO: size on disk for MySQL?
			br:          br,
			doltVersion: job.Version,
		}
	}

	return results
}

func benchmarkLoadData(ctx context.Context, b *testing.B, mConfig sysbench_runner.MysqlConfig, job *ImportBenchmarkJob) {
	dsn, err := sysbench_runner.FormatDsn(mConfig)
	if err != nil {
		log.Fatal(err)
	}

	// TODO make sure this can work on windows
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		rerr := db.Close()
		if err == nil {
			err = rerr
		}
	}()

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf("USE %s", dbName))
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", testTable))
	if err != nil {
		log.Fatal(err)
	}

	// CREATE TABLE
	// TODO: Assumes that table schema uses testTable
	data, err := ioutil.ReadFile(job.SchemaPath)
	if err != nil {
		log.Fatal(err)
	}

	mysql.RegisterLocalFile(job.Filepath)

	_, err = db.ExecContext(ctx, string(data))
	if err != nil {
		log.Fatal(err)
	}

	// TODO: Write a note about replace
	for i := 0; i < b.N; i++ {
		_, err = db.ExecContext(ctx, fmt.Sprintf(`LOAD DATA LOCAL INFILE '%s' REPLACE INTO TABLE %s FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES`, job.Filepath, testTable))
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("MySQL server loaded file %s \n", job.Filepath)
	}
}

// getServerArgs returns the arguments that run the mysql servier
func getServersArgs() []string {
	return []string{"--user=mysql", fmt.Sprintf("--port=%d", defaultPort), "--local-infile=ON"}
}

// getMysqlServer returns a exec.Cmd for a dolt server
func getMysqlServer(ctx context.Context, serverExec string, params []string) *exec.Cmd {
	return execCommand(ctx, serverExec, params...)
}
