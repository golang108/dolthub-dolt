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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/dolthub/dolt/go/performance/utils/sysbench_runner"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

const (
	smallSet  = 100000
	mediumSet = 1000000
	largeSet  = 10000000
	testTable = "test"
)

type ImportBenchmarkJob struct {
	// Name of the job
	Name string

	// NumRows represents the number of rows being imported in the job.
	NumRows int

	// Sorted represents whether the data is sorted or not.
	Sorted bool

	// Format is either csv, json or sql.
	Format string

	// Filepath is the path to the data file. If empty data is generated instead.
	Filepath string

	// Program is either Dolt or MySQL.
	Program string

	// Version tracks the current version of Dolt or MySQL being used
	Version string

	// ExecPath is a path towards a Dolt or MySQL executable. This is also useful when running different versions of Dolt.
	ExecPath string

	// SchemaPath is a path towards a generated schema. It is needed for MySQL testing and optional for Dolt testing
	// TODO: Make sure the schema file is used for Dolt import i.e the -s parameter. Speeds things up!
	SchemaPath string
}

type ImportBenchmarkConfig struct {
	Jobs []*ImportBenchmarkJob

	// MysqlConnectionProtocol is either tcp or unix. On our kubernetes benchmarking deployments unix is needed. To run this
	// locally you want tcp
	MysqlConnectionProtocol string

	// MysqlPort is used to connect with a MySQL port
	MysqlPort int

	// MysqlHost is used to connect with a MySQL host
	MysqlHost string
}

// NewDefaultImportBenchmarkConfig returns a default import configuration where data is generated with accordance to
// the medium set.
func NewDefaultImportBenchmarkConfig() *ImportBenchmarkConfig {
	jobs := []*ImportBenchmarkJob{
		{
			Name:     "dolt_import_small",
			NumRows:  smallSet,
			Sorted:   false,
			Format:   csvExt,
			Version:  "HEAD", // Use whatever dolt is installed locally
			ExecPath: "dolt", // Assumes dolt is installed locally
			Program:  "dolt",
		},
	}

	config := &ImportBenchmarkConfig{
		Jobs: jobs,
	}

	config.updateDefaults()

	return config
}

// FromFileConfig takes in a configuration file (encoded as JSON) and returns the relevant importBenchmark config
func FromFileConfig(configPath string) (*ImportBenchmarkConfig, error) {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	config := &ImportBenchmarkConfig{
		Jobs: make([]*ImportBenchmarkJob, 0),
	}

	err = json.Unmarshal(data, config)
	if err != nil {
		return nil, err
	}

	config.updateDefaults()

	return config, nil
}

func (c *ImportBenchmarkConfig) updateDefaults() {
	if c.MysqlConnectionProtocol == "" {
		c.MysqlConnectionProtocol = "tcp"
	}

	if c.MysqlHost == "" {
		c.MysqlHost = defaultHost
	}

	if c.MysqlPort == 0 {
		c.MysqlPort = defaultPort
	}

	for _, job := range c.Jobs {
		job.updateDefaultsAndValidate()
	}
}

func (j *ImportBenchmarkJob) updateDefaultsAndValidate() {
	j.Program = strings.ToLower(j.Program)

	if j.Program == "mysql" {
		if j.SchemaPath == "" {
			log.Fatalf("error: Must supply schema file for mysql jobs")
		}
	}
}

func getMysqlConfigFromConfig(c *ImportBenchmarkConfig) sysbench_runner.MysqlConfig {
	return sysbench_runner.MysqlConfig{Socket: defaultSocket, Host: c.MysqlHost, ConnectionProtocol: c.MysqlConnectionProtocol, Port: c.MysqlPort}
}

// generateTestFilesIfNeeded creates the test conditions for an import benchmark to execute. In the case that the config
// dictates that data needs to be generated, this function handles that
func generateTestFilesIfNeeded(config *ImportBenchmarkConfig) *ImportBenchmarkConfig {
	jobs := make([]*ImportBenchmarkJob, 0)

	for _, job := range config.Jobs {
		// Preset csv path
		if job.Filepath != "" {
			jobs = append(jobs, job)
		} else {
			filePath, fileFormat := getGeneratedBenchmarkTest(job)

			job.Filepath = filePath
			job.Format = fileFormat

			jobs = append(jobs, job)
		}
	}

	config.Jobs = jobs
	return config
}

// getGeneratedBenchmarkTest is used to create a generated test case with a randomly generated csv file.
func getGeneratedBenchmarkTest(job *ImportBenchmarkJob) (string, string) {
	sch := NewSeedSchema(job.NumRows, genSampleCols(), job.Format)
	testFilePath := generateTestFile(filesys.LocalFS, sch, GetWorkingDir())

	return testFilePath, sch.FileFormatExt
}

func generateTestFile(fs filesys.Filesys, sch *SeedSchema, wd string) string {
	pathToImportFile := filepath.Join(wd, fmt.Sprintf("testData.%s", sch.FileFormatExt))
	wc, err := fs.OpenForWrite(pathToImportFile, os.ModePerm)
	if err != nil {
		panic(err.Error())
	}

	defer wc.Close()

	ds := NewDSImpl(wc, sch, seedRandom, testTable)
	ds.GenerateData()

	return pathToImportFile
}

func RunBenchmarkTests(config *ImportBenchmarkConfig, workingDir string) []result {
	config = generateTestFilesIfNeeded(config)

	// Split into the two jobs because we want
	doltJobs := make([]*ImportBenchmarkJob, 0)
	mySQLJobs := make([]*ImportBenchmarkJob, 0)

	for _, job := range config.Jobs {
		switch strings.ToLower(job.Program) {
		case "dolt":
			doltJobs = append(doltJobs, job)
		case "mysql":
			if job.Format != csvExt {
				log.Fatal("mysql import benchmarking only supports csv files")
			}
			mySQLJobs = append(mySQLJobs, job)
		default:
			log.Fatal("error: Invalid program. Must use dolt or mysql. See the sample config")
		}
	}

	results := make([]result, 0)
	for _, doltJob := range doltJobs {
		results = append(results, BenchmarkDoltImportJob(doltJob, workingDir))
	}

	results = append(results, BenchmarkMySQLImportJobs(mySQLJobs, getMysqlConfigFromConfig(config))...)

	return results
}
