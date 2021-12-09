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

package actions

import (
	"context"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/utils/set"
)

type typeInfoSet map[sql.Type]struct{}

var stringDefaultType = sql.MustCreateStringWithDefaults(sqltypes.VarChar, 16383)

// InferenceArgs are arguments that can be passed to the schema inferrer to modify it's inference behavior.
type InferenceArgs interface {
	// ColNameMapper allows columns named X in the schema to be named Y in the inferred schema.
	ColNameMapper() rowconv.NameMapper
	// FloatThreshold is the threshold at which a string representing a floating point number should be interpreted as
	// a float versus an int.  If FloatThreshold is 0.0 then any number with a decimal point will be interpreted as a
	// float (such as 0.0, 1.0, etc).  If FloatThreshold is 1.0 then any number with a decimal point will be converted
	// to an int (0.5 will be the int 0, 1.99 will be the int 1, etc.  If the FloatThreshold is 0.001 then numbers with
	// a fractional component greater than or equal to 0.001 will be treated as a float (1.0 would be an int, 1.0009 would
	// be an int, 1.001 would be a float, 1.1 would be a float, etc)
	FloatThreshold() float64
}

type inferrer struct {
	readerSch      sql.Schema
	inferSets      map[string]typeInfoSet
	nullable       *set.StrSet
	mapper         rowconv.NameMapper
	floatThreshold float64

	//inferArgs *InferenceArgs
}

func newInferrer(ctx context.Context, readerSch sql.Schema, args InferenceArgs) *inferrer {
	inferSets := make(map[string]typeInfoSet, len(readerSch))

	for _, col := range readerSch {
		inferSets[col.Name] = make(typeInfoSet)
	}

	return &inferrer{
		readerSch:      readerSch,
		inferSets:      inferSets,
		nullable:       set.NewStrSet(nil),
		mapper:         args.ColNameMapper(),
		floatThreshold: args.FloatThreshold(),
	}
}

func (inf *inferrer) inferColumnTypes(ctx context.Context) sql.Schema {
	inferredTypes := make(map[string]sql.Type)
	for colName, typ := range inf.inferSets {
		inferredTypes[inf.mapper.Map(colName)] = findCommonType(typ)
	}

	var ret sql.Schema
	for _, col := range inf.readerSch {
		col.Name = inf.mapper.Map(col.Name)
		col.Type = inferredTypes[col.Name]
		if inf.nullable.Contains(col.Name) {
			col.Nullable = true
		}
		ret = append(ret, col)
	}

	return ret
}

func InferSchemaFromTableReader(ctx context.Context, rd table.TableReadCloser, args InferenceArgs) (sql.PrimaryKeySchema, error) {
	inferrer := newInferrer(ctx, rd.GetSqlSchema().Schema, args)

	// start the pipeline
	g, ctx := errgroup.WithContext(ctx)

	parsedRowChan := make(chan sql.Row)
	g.Go(func() error {
		defer close(parsedRowChan)
		for {
			r, err := rd.ReadSqlRow(ctx)

			if err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case parsedRowChan <- r:
			}
		}
	})

	g.Go(func() error {
		for r := range parsedRowChan {
			for i, col := range rd.GetSqlSchema().Schema {
				val := r[i]
				if val == nil {
					inferrer.nullable.Add(col.Name)
				}
				strVal, err := stringDefaultType.Convert(val)
				if err != nil {
					return err
				}

				if strVal == nil {
					inferrer.inferSets[col.Name][sql.Null] = struct{}{}
				} else {
					typ := leastPermissiveType(strVal.(string), inferrer.floatThreshold)
					inferrer.inferSets[col.Name][typ] = struct{}{}
				}
			}
		}

		return nil
	})

	err := g.Wait()
	if err != nil {
		return sql.PrimaryKeySchema{}, err
	}

	err = rd.Close(ctx)
	if err != nil {
		return sql.PrimaryKeySchema{}, err
	}

	return sql.NewPrimaryKeySchema(inferrer.inferColumnTypes(ctx)), nil
}

func leastPermissiveType(strVal string, floatThreshold float64) sql.Type {
	if len(strVal) == 0 {
		return sql.Null
	}
	strVal = strings.TrimSpace(strVal)

	numType, ok := leastPermissiveNumericType(strVal, floatThreshold)
	if ok {
		return numType
	}

	chronoType, ok := leastPermissiveChronoType(strVal)
	if ok {
		return chronoType
	}

	_, err := uuid.Parse(strVal)
	if err == nil {
		return sql.UUID
	}

	strVal = strings.ToLower(strVal)
	if strVal == "true" || strVal == "false" {
		return sql.Boolean
	}

	return stringDefaultType
}

func leastPermissiveNumericType(strVal string, floatThreshold float64) (ti sql.Type, ok bool) {
	if strings.Contains(strVal, ".") {
		f, err := strconv.ParseFloat(strVal, 64)
		if err != nil {
			return sql.Null, false
		}

		if math.Abs(f) < math.MaxFloat32 {
			ti = sql.Float32
		} else {
			ti = sql.Float64
		}

		if floatThreshold != 0.0 {
			floatParts := strings.Split(strVal, ".")
			decimalPart, err := strconv.ParseFloat("0."+floatParts[1], 64)

			if err != nil {
				panic(err)
			}

			if decimalPart < floatThreshold {
				if ti == sql.Float32 {
					ti = sql.Int32
				} else {
					ti = sql.Int64
				}
			}
		}
		return ti, true
	}

	if strings.Contains(strVal, "-") {
		i, err := strconv.ParseInt(strVal, 10, 64)
		if err != nil {
			return sql.Null, false
		}
		if i >= math.MinInt32 && i <= math.MaxInt32 {
			return sql.Int32, true
		} else {
			return sql.Int64, true
		}
	} else {
		ui, err := strconv.ParseUint(strVal, 10, 64)
		if err != nil {
			return sql.Null, false
		}

		// handle leading zero case
		if len(strVal) > 1 && strVal[0] == '0' {
			return stringDefaultType, true
		}

		if ui <= math.MaxUint32 {
			return sql.Uint32, true
		} else {
			return sql.Uint64, true
		}
	}
}

func leastPermissiveChronoType(strVal string) (sql.Type, bool) {
	if strVal == "" {
		return sql.Null, false
	}

	_, err := sql.Time.Marshal(strVal)
	if err == nil {
		return sql.Time, true
	}

	dt, err := sql.Datetime.Convert(strVal)
	if err != nil {
		return sql.Null, false
	}

	t := dt.(time.Time)
	if t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0 {
		return sql.Date, true
	}

	return sql.Datetime, true
}

func chronoTypes() []sql.Type {
	return []sql.Type{
		sql.Year,
		sql.Date,
		sql.Datetime,
		sql.Time,
		sql.Timestamp,
	}
}

func numericTypes() []sql.Type {
	return []sql.Type{
		sql.Int32,
		sql.Uint32,
		sql.Int64,
		sql.Uint64,
		sql.Float32,
		sql.Float64,
	}
}

// findCommonType takes a set of types and finds the least permissive
// (ie most specific) common type between all types in the set
func findCommonType(ts typeInfoSet) sql.Type {
	// empty values were inferred as UnknownType
	delete(ts, sql.Null)

	if len(ts) == 0 {
		// use strings if all values were empty
		return stringDefaultType
	}

	if len(ts) == 1 {
		for ti := range ts {
			return ti
		}
	}

	// len(ts) > 1

	if _, found := ts[stringDefaultType]; found {
		return stringDefaultType
	}

	hasNumeric := false
	for _, nt := range numericTypes() {
		if setHasType(ts, nt) {
			hasNumeric = true
			break
		}
	}

	hasNonNumeric := false
	for _, nnt := range chronoTypes() {
		if setHasType(ts, nnt) {
			hasNonNumeric = true
			break
		}
	}
	if setHasType(ts, sql.Boolean) || setHasType(ts, sql.UUID) {
		hasNonNumeric = true
	}

	if hasNumeric && hasNonNumeric {
		return stringDefaultType
	}

	if hasNumeric {
		return findCommonNumericType(ts)
	}

	// find a common nonNumeric type

	nonChronoTypes := []sql.Type{
		sql.Boolean,
		sql.MustCreateStringWithDefaults(sqltypes.VarChar, 36),
	}
	for _, nct := range nonChronoTypes {
		if setHasType(ts, nct) {
			// types in nonChronoTypes have only string
			// as a common type with any other type
			return stringDefaultType
		}
	}

	return findCommonChronoType(ts)
}

func findCommonNumericType(nums typeInfoSet) sql.Type {
	// find a common numeric type
	// iterate through types from most to least permissive
	// return the most permissive type found
	//   ints are a subset of floats
	//   uints are a subset of ints
	//   smaller widths are a subset of larger widths
	mostToLeast := []sql.Type{
		sql.Float64,
		sql.Float32,

		// todo: can all Int64 fit in Float64?
		sql.Int64,
		sql.Int32,
		sql.Int24,
		sql.Int16,
		sql.Int8,

		sql.Uint64,
		sql.Uint32,
		sql.Uint24,
		sql.Uint16,
		sql.Uint8,
	}
	for _, numType := range mostToLeast {
		if setHasType(nums, numType) {
			return numType
		}
	}

	panic("unreachable")
}

func findCommonChronoType(chronos typeInfoSet) sql.Type {
	if len(chronos) == 1 {
		for ct := range chronos {
			return ct
		}
	}

	if setHasType(chronos, sql.Datetime) {
		return sql.Datetime
	}

	hasTime := setHasType(chronos, sql.Time) || setHasType(chronos, sql.Timestamp)
	hasDate := setHasType(chronos, sql.Date) || setHasType(chronos, sql.Year)

	if hasTime && !hasDate {
		return sql.Time
	}

	if !hasTime && hasDate {
		return sql.Date
	}

	if hasDate && hasTime {
		return sql.Datetime
	}

	panic("unreachable")
}

func setHasType(ts typeInfoSet, t sql.Type) bool {
	_, found := ts[t]
	return found
}
