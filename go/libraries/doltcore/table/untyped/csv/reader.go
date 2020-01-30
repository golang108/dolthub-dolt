// Copyright 2019 Liquidata, Inc.
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

package csv

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// ReadBufSize is the size of the buffer used when reading the csv file.  It is set at the package level and all
// readers create their own buffer's using the value of this variable at the time they create their buffers.
var ReadBufSize = 256 * 1024

// CSVReader implements TableReader.  It reads csv files and returns rows.
type CSVReader struct {
	closer io.Closer
	bRd    *bufio.Reader
	sch    schema.Schema
	isDone bool
	nbf    *types.NomsBinFormat

	// CSV parsing is based on the standard Golang csv parser in encoding/csv/reader.go
	// This parser has been adapted to differentiate between quoted and unquoted
	// empty strings, and to use multi-rune delimiters. This adaptation removes the
	// comment feature and the lazyQuotes option
	delim   []byte
	numLine int
	fieldsPerRecord int
}

// OpenCSVReader opens a reader at a given path within a given filesys.  The CSVFileInfo should describe the csv file
// being opened.
func OpenCSVReader(nbf *types.NomsBinFormat, path string, fs filesys.ReadableFS, info *CSVFileInfo) (*CSVReader, error) {
	r, err := fs.OpenForRead(path)

	if err != nil {
		return nil, err
	}

	return NewCSVReader(nbf, r, info)
}

// NewCSVReader creates a CSVReader from a given ReadCloser.  The CSVFileInfo should describe the csv file being read.
func NewCSVReader(nbf *types.NomsBinFormat, r io.ReadCloser, info *CSVFileInfo) (*CSVReader, error) {
	if len(info.Delim) < 1 {
		return nil, errors.New(fmt.Sprintf("delimiter '%s' has invalid length", info.Delim))
	}
	if !validDelim(info.Delim) {
		return nil, errors.New(fmt.Sprintf("invalid delimiter: %s", string(info.Delim)))
	}

	br := bufio.NewReaderSize(r, ReadBufSize)
	colStrs, err := getColHeaders(br, info)

	if err != nil {
		r.Close()
		return nil, err
	}

	_, sch := untyped.NewUntypedSchema(colStrs...)

	return &CSVReader{
		closer:          r,
		bRd:             br,
		sch:             sch,
		isDone:          false,
		nbf:             nbf,
		delim:           []byte(info.Delim),
		fieldsPerRecord: sch.GetAllCols().Size(),
	}, nil
}

func getColHeaders(br *bufio.Reader, info *CSVFileInfo) ([]string, error) {
	colStrs := info.Columns
	if info.HasHeaderLine {
		line, _, err := iohelp.ReadLine(br)

		if err != nil {
			return nil, err
		} else if strings.TrimSpace(line) == "" {
			return nil, errors.New("Header line is empty")
		}

		colStrsFromFile, err := csvSplitLine(line, info.Delim, info.EscapeQuotes)

		if err != nil {
			return nil, err
		}

		if colStrs == nil {
			cols := make([]string, len(colStrsFromFile))
			for i := range colStrsFromFile {
				cols[i] = *colStrsFromFile[i]
			}
			colStrs = cols
		}
	}

	return colStrs, nil
}

// ReadRow reads a row from a table.  If there is a bad row the returned error will be non nil, and callin IsBadRow(err)
// will be return true. This is a potentially non-fatal error and callers can decide if they want to continue on a bad row, or fail.
func (csvr *CSVReader) ReadRow(ctx context.Context) (row.Row, error) {
	if csvr.isDone {
		return nil, io.EOF
	}

	colVals, err := csvr.csvReadRecords(nil)

	if err == io.EOF {
		csvr.isDone = true
		return nil, io.EOF
	}

	allCols := csvr.sch.GetAllCols()

	if len(colVals) != allCols.Size() {
		var out strings.Builder
		for _, cv := range colVals {
			if cv != nil {
				out.WriteString(*cv)
			}
			out.WriteRune(',')
		}
		return nil, table.NewBadRow(nil,
			fmt.Sprintf("csv reader's schema expects %d fields, but line only has %d values.", allCols.Size(), len(colVals)),
			fmt.Sprintf("line: '%s'", out.String()),
		)
	}

	if err != nil {
		return nil, table.NewBadRow(nil, err.Error())
	}

	taggedVals := make(row.TaggedValues)
	for i := 0; i < allCols.Size(); i++ {
		col := allCols.GetByIndex(i)
		if colVals[i] == nil {
			taggedVals[col.Tag] = nil
			continue
		}
		taggedVals[col.Tag] = types.String(*colVals[i])
	}

	return row.New(csvr.nbf, csvr.sch, taggedVals)
}

// GetSchema gets the schema of the rows that this reader will return
func (csvr *CSVReader) GetSchema() schema.Schema {
	return csvr.sch
}

// VerifySchema checks that the in schema matches the original schema
func (csvr *CSVReader) VerifySchema(outSch schema.Schema) (bool, error) {
	return schema.VerifyInSchema(csvr.sch, outSch)
}

// Close should release resources being held
func (csvr *CSVReader) Close(ctx context.Context) error {
	if csvr.closer != nil {
		err := csvr.closer.Close()
		csvr.closer = nil

		return err
	} else {
		return errors.New("Already closed.")
	}
}

// Functions below this line are borrowed or adapted from encoding/csv/reader.go

func validDelim(s string) bool {
	return !(strings.Contains(s, "\"") ||
		strings.Contains(s, "\r") ||
		strings.Contains(s, "\n") ||
		strings.Contains(s, string(0xFFFD))) // Unicode replacement char
}

// nextRune returns the next rune in b or utf8.RuneError.
func nextRune(b []byte) rune {
	r, _ := utf8.DecodeRune(b)
	return r
}

func atDelim(line []byte, delim []byte) bool {
	if len(line) < len(delim) {
		return false
	}
	return bytes.Compare(line[:len(delim)], delim) == 0
}

func lengthNL(b []byte) int {
	if len(b) > 0 && b[len(b)-1] == '\n' {
		return 1
	}
	return 0
}

// readLine reads the next line (with the trailing endline).
// If EOF is hit without a trailing endline, it will be omitted.
// If some bytes were read, then the error is never io.EOF.
// The result is only valid until the next call to readLine.
func (csvr *CSVReader) readLine() ([]byte, error) {
	// rawBuffer is a line buffer only used by the readLine method.
	var rawBuffer []byte

	line, err := csvr.bRd.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		rawBuffer = append(rawBuffer[:0], line...)
		for err == bufio.ErrBufferFull {
			line, err = csvr.bRd.ReadSlice('\n')
			rawBuffer = append(rawBuffer, line...)
		}
		line = rawBuffer
	}
	if len(line) > 0 && err == io.EOF {
		err = nil
		// For backwards compatibility, drop trailing \r before EOF.
		if line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
	}
	csvr.numLine++
	// Normalize \r\n to \n on all input lines.
	if n := len(line); n >= 2 && line[n-2] == '\r' && line[n-1] == '\n' {
		line[n-2] = '\n'
		line = line[:n-1]
	}
	return line, err
}

func (csvr *CSVReader) csvReadRecords(dst []*string) ([]*string, error) {
	// recordBuffer holds the unescaped fields, one after another.
	// The fields can be accessed by using the indexes in fieldIndexes.
	// E.g., For the row `a,"b","c""d",e`, recordBuffer will contain `abc"de`
	// and fieldIndexes will contain the indexes [1, 2, 5, 6].
	var recordBuffer    []byte
	var fieldIndexes    []int

	var keepString []bool

	// Read line (automatically skipping past empty lines and any comments).
	var line, fullLine []byte
	var errRead error
	for errRead == nil {
		line, errRead = csvr.readLine()
		if errRead == nil && len(line) == lengthNL(line) {
			line = nil
			continue // Skip empty lines
		}
		fullLine = line
		break
	}
	if errRead == io.EOF {
		return nil, errRead
	}

	// Parse each field in the record.
	var err error
	const quoteLen = len(`"`)
	delimLen := len(csvr.delim)
	recLine := csvr.numLine // Starting line for record
	recordBuffer = recordBuffer[:0]
	fieldIndexes = fieldIndexes[:0]
parseField:
	for {
		line = bytes.TrimLeftFunc(line, unicode.IsSpace)
		if len(line) == 0 || line[0] != '"' {
			// Non-quoted string field
			i := bytes.Index(line, csvr.delim)
			field := line
			if i >= 0 {
				field = field[:i]
			} else {
				field = field[:len(field)-lengthNL(field)]
			}
			recordBuffer = append(recordBuffer, field...)
			fieldIndexes = append(fieldIndexes, len(recordBuffer))
			// discard unquoted empty strings
			keepString = append(keepString, len(field) != 0)
			if i >= 0 {
				line = line[i+delimLen:]
				continue parseField
			}
			break parseField
		} else {
			// Quoted string field
			line = line[quoteLen:]
			for {
				i := bytes.IndexByte(line, '"')
				if i >= 0 {
					// Hit next quote.
					recordBuffer = append(recordBuffer, line[:i]...)
					line = line[i+quoteLen:]
					if atDelim(line, csvr.delim) {
						// `"<delimiter>` sequence (end of field).
						line = line[delimLen:]
						fieldIndexes = append(fieldIndexes, len(recordBuffer))
						keepString = append(keepString, true)
						continue parseField
					} else {
						switch rn := nextRune(line); {
						case rn == '"':
							// `""` sequence (append quote).
							recordBuffer = append(recordBuffer, '"')
							line = line[quoteLen:]
						case lengthNL(line) == len(line):
							// `"\n` sequence (end of line).
							fieldIndexes = append(fieldIndexes, len(recordBuffer))
							keepString = append(keepString, true)
							break parseField
						default:
							// `"*` sequence (invalid non-escaped quote).
							col := utf8.RuneCount(fullLine[:len(fullLine)-len(line)-quoteLen])
							err = &csv.ParseError{StartLine: recLine, Line: csvr.numLine, Column: col, Err: csv.ErrQuote}
							break parseField
						}
					}
				} else if len(line) > 0 {
					// Hit end of line (copy all data so far).
					recordBuffer = append(recordBuffer, line...)
					if errRead != nil {
						break parseField
					}
					line, errRead = csvr.readLine()
					if errRead == io.EOF {
						errRead = nil
					}
					fullLine = line
				} else {
					// Abrupt end of file (EOF or error).
					if errRead == nil {
						col := utf8.RuneCount(fullLine)
						err = &csv.ParseError{StartLine: recLine, Line: csvr.numLine, Column: col, Err: csv.ErrQuote}
						break parseField
					}
					fieldIndexes = append(fieldIndexes, len(recordBuffer))
					break parseField
				}
			}
		}
	}
	if err == nil {
		err = errRead
	}

	// Create a single string and create slices out of it.
	// This pins the memory of the fields together, but allocates once.
	str := string(recordBuffer) // Convert to string once to batch allocations
	dst = dst[:0]
	if cap(dst) < len(fieldIndexes) {
		dst = make([]*string, len(fieldIndexes))
	}
	dst = dst[:len(fieldIndexes)]
	var preIdx int
	for i, idx := range fieldIndexes {
		if keepString[i] {
			s := str[preIdx:idx]
			dst[i] = &s
		} else {
			dst[i] = nil
		}
		preIdx = idx
	}

	// Check or update the expected fields per record.
	if csvr.fieldsPerRecord > 0 {
		if len(dst) != csvr.fieldsPerRecord && err == nil {
			err = &csv.ParseError{StartLine: recLine, Line: recLine, Err: csv.ErrFieldCount}
		}
	} else if csvr.fieldsPerRecord == 0 {
		csvr.fieldsPerRecord = len(dst)
	}

	return dst, err
}
