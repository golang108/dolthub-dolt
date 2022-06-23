// Copyright 2020 Dolthub, Inc.
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

package cvcmds

import (
	"context"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/types"
)

var verifyConstraintsDocs = cli.CommandDocumentationContent{
	ShortDesc: `Verifies that working set changes satisfy table constraints`,
	LongDesc: `Verifies that inserted or modified rows in the working set satisfy the defined table constraints.
               If any constraints are violated, they are documented in the dolt_constraint_violations system table.
               By default, this command does not consider row changes that have been previously committed.`,
	Synopsis: []string{`[--all] [--output-only] [{{.LessThan}}table{{.GreaterThan}}...]`},
}

type VerifyConstraintsCmd struct{}

var _ cli.Command = VerifyConstraintsCmd{}

func (cmd VerifyConstraintsCmd) Name() string {
	return "verify"
}

func (cmd VerifyConstraintsCmd) Description() string {
	return "Command to verify that the constraints on the given table(s) are satisfied."
}

func (cmd VerifyConstraintsCmd) GatedForNBF(nbf *types.NomsBinFormat) bool {
	return types.IsFormat_DOLT_1(nbf)
}

func (cmd VerifyConstraintsCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(verifyConstraintsDocs, ap)
}

func (cmd VerifyConstraintsCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateVerifyConstraintsArgParser()
}

func (cmd VerifyConstraintsCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, verifyConstraintsDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	verifyAllRows := apr.Contains(cli.AllFlag)
	outputOnly := apr.Contains(cli.OutputOnlyFlag)
	working, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("Unable to get working.").AddCause(err).Build(), nil)
	}
	tableNames := apr.Args
	if len(tableNames) == 0 {
		tableNames, err = working.GetTableNames(ctx)
		if err != nil {
			return commands.HandleVErrAndExitCode(errhand.BuildDError("Unable to read table names.").AddCause(err).Build(), nil)
		}
	}
	tableSet := set.NewStrSet(tableNames)

	comparingRoot, err := dEnv.HeadRoot(ctx)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("Unable to get head root.").AddCause(err).Build(), nil)
	}
	if verifyAllRows {
		comparingRoot, err = doltdb.EmptyRootValue(ctx, comparingRoot.VRW())
		if err != nil {
			return commands.HandleVErrAndExitCode(errhand.BuildDError("Unable to create an empty root.").AddCause(err).Build(), nil)
		}
	}

	cm, err := dEnv.HeadCommit(ctx)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("Unable to get head commit.").AddCause(err).Build(), nil)
	}
	h, err := cm.HashOf()
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("Unable to get head commit hash.").AddCause(err).Build(), nil)
	}

	endRoot, tablesWithViolations, err := merge.AddConstraintViolations(ctx, working, comparingRoot, tableSet, h)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("Unable to process constraint violations.").AddCause(err).Build(), nil)
	}
	if !outputOnly {
		err = dEnv.UpdateWorkingRoot(ctx, endRoot)
		if err != nil {
			return commands.HandleVErrAndExitCode(errhand.BuildDError("Unable to update working root.").AddCause(err).Build(), nil)
		}
	}

	if tablesWithViolations.Size() > 0 {
		cli.PrintErrln("All constraints are not satisfied.")
		for _, tableName := range tablesWithViolations.AsSortedSlice() {
			table, ok, err := endRoot.GetTable(ctx, tableName)
			if err != nil {
				return commands.HandleVErrAndExitCode(errhand.BuildDError("Error loading table.").AddCause(err).Build(), nil)
			}
			if !ok {
				return commands.HandleVErrAndExitCode(errhand.BuildDError("Unable to load table '%s'.", tableName).Build(), nil)
			}
			cvSch, err := table.GetConstraintViolationsSchema(ctx)
			if err != nil {
				return commands.HandleVErrAndExitCode(errhand.BuildDError("Error loading constraint violations schema.").AddCause(err).Build(), nil)
			}
			cvMap, err := table.GetConstraintViolations(ctx)
			if err != nil {
				return commands.HandleVErrAndExitCode(errhand.BuildDError("Error loading constraint violations data.").AddCause(err).Build(), nil)
			}
			sqlSchema, err := sqlutil.FromDoltSchema(tableName, cvSch)
			if err != nil {
				return commands.HandleVErrAndExitCode(errhand.BuildDError("Error attempting to convert schema").AddCause(err).Build(), nil)
			}
			rowIter, err := sqlutil.MapToSqlIter(ctx, cvSch, cvMap)
			if err != nil {
				return commands.HandleVErrAndExitCode(errhand.BuildDError("Error attempting to create row iterator").AddCause(err).Build(), nil)
			}
			cli.Println("")
			cli.Println(doltdb.DoltConstViolTablePrefix + tableName)
			if cvMap.Len() > 50 {
				cli.Printf("Over 50 constraint violations were found. Please query '%s' to see them all.\n", doltdb.DoltConstViolTablePrefix+tableName)
			} else {
				err = engine.PrettyPrintResults(sql.NewEmptyContext(), engine.FormatTabular, sqlSchema.Schema, rowIter, false)
				if err != nil {
					return commands.HandleVErrAndExitCode(errhand.BuildDError("Error outputting rows").AddCause(err).Build(), nil)
				}
			}
		}
		return 1
	}
	return 0
}
