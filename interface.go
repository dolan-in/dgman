package dgman

import (
	"context"

	"github.com/dgraph-io/dgo/v200"
)

// TxnInterface provides interface for dgman.TxnContext
type TxnInterface interface {
	Commit() error
	Discard() error
	BestEffort() *TxnContext
	Txn() *dgo.Txn
	WithContext(context.Context)
	Context() context.Context
	Mutate(data interface{}, commitNow ...bool) error
	Create(data interface{}, commitNow ...bool) error
	Update(data interface{}, commitNow ...bool) error
	Upsert(data interface{}, predicate string, commitNow ...bool) error
	CreateOrGet(data interface{}, predicate string, commitNow ...bool) error
	Delete(model interface{}, commitNow ...bool) *Deleter
	Get(model interface{}) *Query
}

// SchemaType allows defining a custom type as a dgraph schema type
type SchemaType interface {
	SchemaType() string
}

var (
	_ TxnInterface = (*TxnContext)(nil)
)
