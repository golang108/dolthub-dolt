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

package prolly

import (
	"context"
	"io"

	"github.com/dolthub/dolt/go/store/val"
)

type MapIter interface {
	Next(ctx context.Context) (key, value val.Tuple, err error)
}

type Range struct {
	lowKey, highKey val.Tuple
	inclusiveLow    bool
	inclusiveHigh   bool
	reverse         bool

	// hack
	Point val.Tuple
}

type valueIter struct {
	rng  Range
	cur  nodeCursor
	done bool
}

func (it *valueIter) Next(ctx context.Context) (key, value val.Tuple, err error) {
	if it.rng.Point == nil {
		panic("only point lookups are supported")
	}
	if it.done {
		return nil, nil, io.EOF
	}
	it.done = true

	key = val.Tuple(it.cur.current())
	if _, err = it.cur.advance(ctx); err != nil {
		return
	}
	value = val.Tuple(it.cur.current())

	return
}

// IndexRange is an inclusive range of item indexes
type IndexRange struct {
	Low, High uint64
	Reverse   bool
}

type indexIter struct {
	rng IndexRange
	cur nodeCursor
	rem uint64
}

func (it *indexIter) Next(ctx context.Context) (key, value val.Tuple, err error) {
	if it.rem == 0 {
		return nil, nil, io.EOF
	}

	key = val.Tuple(it.cur.current())
	if _, err = it.cur.advance(ctx); err != nil {
		return nil, nil, err
	}
	value = val.Tuple(it.cur.current())

	if it.rng.Reverse {
		for i := 0; i < 3; i++ {
			if _, err = it.cur.retreat(ctx); err != nil {
				return nil, nil, err
			}
		}
	} else {
		if _, err = it.cur.advance(ctx); err != nil {
			return nil, nil, err
		}
	}

	it.rem--
	return
}
