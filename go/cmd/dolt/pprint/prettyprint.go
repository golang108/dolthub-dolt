// Copyright 2021 Dolthub, Inc.
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

package pprint

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/csv"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/fwt"
	"github.com/dolthub/dolt/go/libraries/utils/pipeline"
)

type ResultFormat byte
type CliOutput byte

const (
	FormatTabular ResultFormat = iota
	FormatCsv
	FormatJson
	FormatNull // used for profiling
)

const (
	StdOutput CliOutput = iota
	StdError
)

func PrettyPrintResults(ctx *sql.Context, resultFormat ResultFormat, sqlSch sql.Schema, rowIter sql.RowIter, output CliOutput) (rerr error) {
	defer func() {
		closeErr := rowIter.Close(ctx)
		if rerr == nil && closeErr != nil {
			rerr = closeErr
		}
	}()

	if isOkResult(sqlSch) {
		return printOKResult(rowIter)
	}

	// For some output formats, we want to convert everything to strings to be processed by the pipeline. For others,
	// we want to leave types alone and let the writer figure out how to format it for output.
	var p *pipeline.Pipeline
	switch resultFormat {
	case FormatCsv:
		p = createCSVPipeline(ctx, sqlSch, rowIter, output)
	case FormatJson:
		p = createJSONPipeline(ctx, sqlSch, rowIter, output)
	case FormatTabular:
		p = createTabularPipeline(ctx, sqlSch, rowIter, output)
	case FormatNull:
		p = createNullPipeline(ctx, sqlSch, rowIter)
	}

	p.Start(ctx)
	rerr = p.Wait()

	return rerr
}

func printOKResult(iter sql.RowIter) (returnErr error) {
	row, err := iter.Next()
	if err != nil {
		return err
	}

	if okResult, ok := row[0].(sql.OkResult); ok {
		rowNoun := "row"
		if okResult.RowsAffected != 1 {
			rowNoun = "rows"
		}

		fmt.Fprintf(color.Output, "Query OK, %d %s affected\n", okResult.RowsAffected, rowNoun)

		if okResult.Info != nil {
			fmt.Fprintf(color.Output, "%s\n", okResult.Info)
		}
	}

	return nil
}

func isOkResult(sch sql.Schema) bool {
	return sch.Equals(sql.OkResultSchema)
}

const (
	readBatchSize  = 10
	writeBatchSize = 1
)

// noParallelizationInitFunc only exists to validate the routine wasn't parallelized
func noParallelizationInitFunc(ctx context.Context, index int) error {
	if index != 0 {
		panic("cannot parallelize this routine")
	}

	return nil
}

// sqlColToStr is a utility function for converting a sql column of type interface{} to a string
func sqlColToStr(col interface{}) string {
	if col != nil {
		switch typedCol := col.(type) {
		case int:
			return strconv.FormatInt(int64(typedCol), 10)
		case int32:
			return strconv.FormatInt(int64(typedCol), 10)
		case int64:
			return strconv.FormatInt(int64(typedCol), 10)
		case int16:
			return strconv.FormatInt(int64(typedCol), 10)
		case int8:
			return strconv.FormatInt(int64(typedCol), 10)
		case uint:
			return strconv.FormatUint(uint64(typedCol), 10)
		case uint32:
			return strconv.FormatUint(uint64(typedCol), 10)
		case uint64:
			return strconv.FormatUint(uint64(typedCol), 10)
		case uint16:
			return strconv.FormatUint(uint64(typedCol), 10)
		case uint8:
			return strconv.FormatUint(uint64(typedCol), 10)
		case float64:
			return strconv.FormatFloat(float64(typedCol), 'g', -1, 64)
		case float32:
			return strconv.FormatFloat(float64(typedCol), 'g', -1, 32)
		case string:
			return typedCol
		case bool:
			if typedCol {
				return "true"
			} else {
				return "false"
			}
		case time.Time:
			return typedCol.Format("2006-01-02 15:04:05.999999 -0700 MST")
		}
	}

	return ""
}

// getReadStageFunc is a general purpose stage func used by multiple pipelines to read the rows into batches
func getReadStageFunc(iter sql.RowIter, batchSize int) pipeline.StageFunc {
	isDone := false
	return func(ctx context.Context, _ []pipeline.ItemWithProps) ([]pipeline.ItemWithProps, error) {
		if isDone {
			return nil, io.EOF
		}

		items := make([]pipeline.ItemWithProps, 0, batchSize)
		for i := 0; i < 10; i++ {
			r, err := iter.Next()

			if err == io.EOF {
				isDone = true
				break
			} else if err != nil {
				return nil, err
			}

			items = append(items, pipeline.NewItemWithNoProps(r))
		}

		if len(items) == 0 {
			return nil, io.EOF
		}

		return items, nil
	}
}

// writeToCliOutStageFunc is a general purpose stage func to write the output of a pipeline to stdout
func writeToCliOutStageFunc(ctx context.Context, items []pipeline.ItemWithProps) ([]pipeline.ItemWithProps, error) {
	if items == nil {
		return nil, nil
	}

	for _, item := range items {
		str := *item.GetItem().(*string)
		fmt.Fprint(color.Output, str)
	}

	return nil, nil
}

// writeToCliErrStageFunc is a general purpose stage func to write the output of a pipeline to stderr
func writeToCliErrStageFunc(_ context.Context, items []pipeline.ItemWithProps) ([]pipeline.ItemWithProps, error) {
	if items == nil {
		return nil, nil
	}

	for _, item := range items {
		str := *item.GetItem().(*string)
		fmt.Fprint(color.Error, str)
	}

	return nil, nil
}

// Null pipeline creation and stage functions

func createNullPipeline(_ context.Context, sch sql.Schema, iter sql.RowIter) *pipeline.Pipeline {
	return pipeline.NewPipeline(
		pipeline.NewStage("read", noParallelizationInitFunc, getReadStageFunc(iter, readBatchSize), 0, 0, 0),
		pipeline.NewStage("drop", nil, dropOnFloor, 0, 100, writeBatchSize),
	)
}

func dropOnFloor(ctx context.Context, items []pipeline.ItemWithProps) ([]pipeline.ItemWithProps, error) {
	return nil, nil
}

// CSV Pipeline creation and stage functions

func createCSVPipeline(ctx context.Context, sch sql.Schema, iter sql.RowIter, output CliOutput) *pipeline.Pipeline {
	var stages = []*pipeline.Stage{
		pipeline.NewStage("read", noParallelizationInitFunc, getReadStageFunc(iter, readBatchSize), 0, 0, 0),
		pipeline.NewStage("process", nil, csvProcessStageFunc, 2, 1000, readBatchSize),
	}

	if output == StdOutput {
		stages = append(stages, pipeline.NewStage("write", noParallelizationInitFunc, writeToCliOutStageFunc, 0, 100, writeBatchSize))
	} else {
		stages = append(stages, pipeline.NewStage("write", noParallelizationInitFunc, writeToCliErrStageFunc, 0, 100, writeBatchSize))
	}

	p := pipeline.NewPipeline(stages...)

	writeIn, _ := p.GetInputChannel("write")
	sb := strings.Builder{}
	for i, col := range sch {
		if i != 0 {
			sb.WriteRune(',')
		}

		sb.WriteString(col.Name)
	}
	sb.WriteRune('\n')

	str := sb.String()
	writeIn <- []pipeline.ItemWithProps{pipeline.NewItemWithNoProps(&str)}

	return p
}

func csvProcessStageFunc(ctx context.Context, items []pipeline.ItemWithProps) ([]pipeline.ItemWithProps, error) {
	if items == nil {
		return nil, nil
	}

	var b bytes.Buffer
	wr := bufio.NewWriter(&b)

	for _, item := range items {
		r := item.GetItem().(sql.Row)
		colValStrs := make([]*string, len(r))

		for colNum, col := range r {
			if col != nil {
				str := sqlColToStr(col)
				colValStrs[colNum] = &str
			} else {
				colValStrs[colNum] = nil
			}
		}

		err := csv.WriteCSVRow(wr, colValStrs, ",", false)

		if err != nil {
			return nil, err
		}
	}

	wr.Flush()

	str := b.String()
	return []pipeline.ItemWithProps{pipeline.NewItemWithNoProps(&str)}, nil
}

// JSON pipeline creation and stage functions
func createJSONPipeline(_ context.Context, sch sql.Schema, iter sql.RowIter, output CliOutput) *pipeline.Pipeline {
	var stages = []*pipeline.Stage{
		pipeline.NewStage("read", noParallelizationInitFunc, getReadStageFunc(iter, readBatchSize), 0, 0, 0),
		pipeline.NewStage("process", nil, getJSONProcessFunc(sch), 2, 1000, readBatchSize),
	}

	if output == StdOutput {
		stages = append(stages, pipeline.NewStage("write", noParallelizationInitFunc, writeJSONToCliOutStageFunc, 0, 100, writeBatchSize))
	} else {
		stages = append(stages, pipeline.NewStage("write", noParallelizationInitFunc, writeJSONToCliErrStageFunc, 0, 100, writeBatchSize))
	}

	p := pipeline.NewPipeline(stages...)

	return p
}

func getJSONProcessFunc(sch sql.Schema) pipeline.StageFunc {
	formats := make([]string, len(sch))
	for i, col := range sch {
		switch col.Type.(type) {
		case sql.StringType, sql.DatetimeType, sql.EnumType, sql.TimeType:
			formats[i] = fmt.Sprintf(`"%s":"%%s"`, col.Name)
		default:
			formats[i] = fmt.Sprintf(`"%s":%%s`, col.Name)
		}
	}

	return func(ctx context.Context, items []pipeline.ItemWithProps) ([]pipeline.ItemWithProps, error) {
		if items == nil {
			return nil, nil
		}

		sb := &strings.Builder{}
		sb.Grow(2048)
		for i, item := range items {
			r := item.GetItem().(sql.Row)

			if i != 0 {
				sb.WriteString(",{")
			} else {
				sb.WriteString("{")
			}

			validCols := 0
			for colNum, col := range r {
				if col != nil {
					if validCols != 0 {
						sb.WriteString(",")
					}

					validCols++
					colStr := sqlColToStr(col)
					colStr = strings.Replace(colStr, "\"", "\\\"", -1)
					str := fmt.Sprintf(formats[colNum], colStr)
					sb.WriteString(str)
				}
			}

			sb.WriteRune('}')
		}

		str := sb.String()
		return []pipeline.ItemWithProps{pipeline.NewItemWithNoProps(&str)}, nil
	}
}

func writeJSONToCliOutStageFunc(ctx context.Context, items []pipeline.ItemWithProps) ([]pipeline.ItemWithProps, error) {
	const hasRunKey = 0

	ls := pipeline.GetLocalStorage(ctx)
	_, hasRun := ls.Get(hasRunKey)
	ls.Put(hasRunKey, true)

	if items == nil {
		if hasRun {
			fmt.Fprint(color.Output, "]}")
		} else {
			fmt.Fprint(color.Output, "{\"rows\":[]}")
		}
	} else {
		for _, item := range items {
			if hasRun {
				fmt.Fprint(color.Output, ",")
			} else {
				fmt.Fprint(color.Output, "{\"rows\": [")
			}

			str := *item.GetItem().(*string)
			fmt.Fprint(color.Output, str)

			hasRun = true
		}
	}

	return nil, nil
}

func writeJSONToCliErrStageFunc(ctx context.Context, items []pipeline.ItemWithProps) ([]pipeline.ItemWithProps, error) {
	const hasRunKey = 0

	ls := pipeline.GetLocalStorage(ctx)
	_, hasRun := ls.Get(hasRunKey)
	ls.Put(hasRunKey, true)

	if items == nil {
		if hasRun {
			fmt.Fprint(color.Error, "]}")
		} else {
			fmt.Fprint(color.Error, "{\"rows\":[]}")
		}
	} else {
		for _, item := range items {
			if hasRun {
				fmt.Fprint(color.Error, ",")
			} else {
				fmt.Fprint(color.Error, "{\"rows\": [")
			}

			str := *item.GetItem().(*string)
			fmt.Fprint(color.Error, str)

			hasRun = true
		}
	}

	return nil, nil
}

// tabular pipeline creation and pipeline functions
func createTabularPipeline(_ context.Context, sch sql.Schema, iter sql.RowIter, output CliOutput) *pipeline.Pipeline {
	const samplesForAutoSizing = 10000
	tps := &tabularPipelineStages{}

	var stages = []*pipeline.Stage{
		pipeline.NewStage("read", nil, getReadStageFunc(iter, readBatchSize), 0, 0, 0),
		pipeline.NewStage("stringify", nil, rowsToStringSlices, 0, 1000, 1000),
		pipeline.NewStage("fix_width", noParallelizationInitFunc, tps.getFixWidthStageFunc(samplesForAutoSizing), 0, 1000, readBatchSize),
		pipeline.NewStage("cell_borders", noParallelizationInitFunc, tps.getBorderFunc(), 0, 1000, readBatchSize),
	}

	if output == StdOutput {
		stages = append(stages, pipeline.NewStage("write", noParallelizationInitFunc, writeToCliOutStageFunc, 0, 100, writeBatchSize))
	} else {
		stages = append(stages, pipeline.NewStage("write", noParallelizationInitFunc, writeToCliErrStageFunc, 0, 100, writeBatchSize))
	}

	p := pipeline.NewPipeline(stages...)

	writeIn, _ := p.GetInputChannel("fix_width")
	headers := make([]string, len(sch))
	for i, col := range sch {
		headers[i] = col.Name
	}

	writeIn <- []pipeline.ItemWithProps{
		pipeline.NewItemWithProps(headers, pipeline.NewImmutableProps(map[string]interface{}{"headers": true})),
	}

	return p
}

func rowsToStringSlices(_ context.Context, items []pipeline.ItemWithProps) ([]pipeline.ItemWithProps, error) {
	if items == nil {
		return nil, nil
	}

	rows := make([]pipeline.ItemWithProps, len(items))
	for i, item := range items {
		r := item.GetItem().(sql.Row)

		cols := make([]string, len(r))
		for colNum, col := range r {
			isNull := col == nil

			if !isNull {
				sqlTypeInst, isType := col.(sql.Type)

				if isType && sqlTypeInst.Type() == sqltypes.Null {
					isNull = true
				}
			}

			if !isNull {
				cols[colNum] = sqlColToStr(col)
			} else {
				cols[colNum] = "NULL"
			}
		}

		rows[i] = pipeline.NewItemWithNoProps(cols)
	}

	return rows, nil
}

type tabularPipelineStages struct {
	rowSep string
}

func (tps *tabularPipelineStages) getFixWidthStageFunc(samples int) func(context.Context, []pipeline.ItemWithProps) ([]pipeline.ItemWithProps, error) {
	bufferring := true
	buffer := make([]pipeline.ItemWithProps, 0, samples)
	idxToMaxWidth := make(map[int]int)
	idxToMaxNumRunes := make(map[int]int)
	var fwf fwt.FixedWidthFormatter
	return func(_ context.Context, items []pipeline.ItemWithProps) ([]pipeline.ItemWithProps, error) {
		if items == nil {
			bufferring = false
			fwf = fwt.NewFixedWidthFormatter(fwt.HashFillWhenTooLong, idxMapToSlice(idxToMaxWidth), idxMapToSlice(idxToMaxNumRunes))
			tps.rowSep = genRowSepString(fwf)
			return tps.formatItems(fwf, buffer)
		}

		if bufferring {
			for _, item := range items {
				cols := item.GetItem().([]string)

				for colIdx, colStr := range cols {
					strWidth := fwt.StringWidth(colStr)
					if strWidth > idxToMaxWidth[colIdx] {
						idxToMaxWidth[colIdx] = strWidth
					}

					numRunes := len([]rune(colStr))
					if numRunes > idxToMaxNumRunes[colIdx] {
						idxToMaxNumRunes[colIdx] = numRunes
					}
				}

				buffer = append(buffer, item)
			}

			if len(buffer) > samples {
				bufferring = false
				fwf = fwt.NewFixedWidthFormatter(fwt.HashFillWhenTooLong, idxMapToSlice(idxToMaxWidth), idxMapToSlice(idxToMaxNumRunes))
				tps.rowSep = genRowSepString(fwf)
				return tps.formatItems(fwf, buffer)
			}

			return nil, nil
		}

		return tps.formatItems(fwf, items)
	}
}

func (tps *tabularPipelineStages) formatItems(fwf fwt.FixedWidthFormatter, items []pipeline.ItemWithProps) ([]pipeline.ItemWithProps, error) {
	results := make([]pipeline.ItemWithProps, len(items))
	for i, item := range items {
		cols := item.GetItem().([]string)
		formatted, err := fwf.Format(cols)

		if err != nil {
			return nil, err
		}

		results[i] = pipeline.NewItemWithProps(formatted, item.GetProperties())
	}

	return results, nil
}

func (tps *tabularPipelineStages) getBorderFunc() func(context.Context, []pipeline.ItemWithProps) ([]pipeline.ItemWithProps, error) {
	return func(_ context.Context, items []pipeline.ItemWithProps) ([]pipeline.ItemWithProps, error) {
		if items == nil {
			return []pipeline.ItemWithProps{pipeline.NewItemWithNoProps(&tps.rowSep)}, nil
		}

		sb := &strings.Builder{}
		sb.Grow(2048)
		for _, item := range items {
			props := item.GetProperties()
			headers := false
			if _, ok := props.Get("headers"); ok {
				headers = true
				sb.WriteString(tps.rowSep)
			}

			cols := item.GetItem().([]string)

			for _, str := range cols {
				sb.WriteString("| ")
				sb.WriteString(str)
				sb.WriteRune(' ')
			}

			sb.WriteString("|\n")

			if headers {
				sb.WriteString(tps.rowSep)
			}
		}

		str := sb.String()
		return []pipeline.ItemWithProps{pipeline.NewItemWithNoProps(&str)}, nil
	}
}

func idxMapToSlice(idxMap map[int]int) []int {
	sl := make([]int, len(idxMap))
	for idx, val := range idxMap {
		sl[idx] = val
	}

	return sl
}

func genRowSepString(fwf fwt.FixedWidthFormatter) string {
	rowSepRunes := make([]rune, fwf.TotalWidth+(3*len(fwf.Widths))+2)
	for i := 0; i < len(rowSepRunes); i++ {
		rowSepRunes[i] = '-'
	}

	var pos int
	for _, width := range fwf.Widths {
		rowSepRunes[pos] = '+'
		pos += width + 3
	}

	rowSepRunes[pos] = '+'
	rowSepRunes[pos+1] = '\n'

	return string(rowSepRunes)
}
