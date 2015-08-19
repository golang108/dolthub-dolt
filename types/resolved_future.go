package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

type resolvedFuture struct {
	val Value
}

func (rf resolvedFuture) Ref() ref.Ref {
	return rf.val.Ref()
}

func (rf resolvedFuture) Val() Value {
	return rf.val
}

func (rf resolvedFuture) Deref(cs chunks.ChunkSource) Value {
	return rf.val
}

func (rf resolvedFuture) Release() {
}
