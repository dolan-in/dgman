package dgman

import (
	"context"
	"fmt"

	"github.com/dgraph-io/dgo/v200"
)

// TxnContext is dgo transaction coupled with context
type TxnContext struct {
	txn *dgo.Txn
	ctx context.Context
}

// Commit calls Commit on the dgo transaction.
func (t *TxnContext) Commit() error {
	return t.txn.Commit(t.ctx)
}

// Discard calls Discard on the dgo transaction.
func (t *TxnContext) Discard() error {
	return t.txn.Discard(t.ctx)
}

// BestEffort enables best effort in read-only queries.
func (t *TxnContext) BestEffort() *TxnContext {
	t.txn.BestEffort()
	return t
}

// Txn returns the dgo transaction
func (t *TxnContext) Txn() *dgo.Txn {
	return t.txn
}

// WithContext replaces the current transaction context
func (t *TxnContext) WithContext(ctx context.Context) {
	t.ctx = ctx
}

// Context returns the transaction context
func (t *TxnContext) Context() context.Context {
	return t.ctx
}

// Mutate is a shortcut to create mutations from data to be marshalled into JSON,
// it will inject the node type from the Struct name
func (t *TxnContext) Mutate(data interface{}, commitNow ...bool) error {
	optCommitNow := false
	if len(commitNow) > 0 {
		optCommitNow = commitNow[0]
	}

	mType, err := newMutateType(data)
	if err != nil {
		return err
	}

	assigned, err := mutate(t.ctx, t.txn, data, optCommitNow)
	if err != nil {
		return err
	}

	return (&mutation{mType: mType}).saveUID(assigned.Uids)
}

// Create create node(s) with field unique checking, similar to Mutate,
// will inject node type from the Struct name
func (t *TxnContext) Create(data interface{}, commitNow ...bool) error {
	mutation, err := newMutation(t, data, commitNow...)
	if err != nil {
		return err
	}
	return mutation.do()
}

// Update updates a node by their UID with field unique checking, similar to Mutate,
// will inject node type from the Struct name
func (t *TxnContext) Update(data interface{}, commitNow ...bool) error {
	mutation, err := newMutation(t, data, commitNow...)
	if err != nil {
		return err
	}
	mutation.update = true
	return mutation.do()
}

// Upsert will update a node when a value from the passed predicate (with the node type) exists, otherwise insert the node.
// On all conditions, unique checking holds on the node type on other unique fields.
func (t *TxnContext) Upsert(data interface{}, predicate string, commitNow ...bool) error {
	mutation, err := newMutation(t, data, commitNow...)
	if err != nil {
		return err
	}
	if _, exists := mutation.mType.predIndex[predicate]; !exists {
		return fmt.Errorf("predicate \"%s\" does not exist in passed data", predicate)
	}
	mutation.predicate = predicate
	return mutation.do()
}

// CreateOrGet will create a node or if a node with a value from the passed predicate exists, return the node
func (t *TxnContext) CreateOrGet(data interface{}, predicate string, commitNow ...bool) error {
	mutation, err := newMutation(t, data, commitNow...)
	if err != nil {
		return err
	}
	if _, exists := mutation.mType.predIndex[predicate]; !exists {
		return fmt.Errorf("predicate \"%s\" does not exist in passed data", predicate)
	}
	mutation.returnQuery = true
	mutation.predicate = predicate
	return mutation.do()
}

// Delete prepares a delete mutation using a query
func (t *TxnContext) Delete(model interface{}, commitNow ...bool) *Deleter {
	optCommitNow := false
	if len(commitNow) > 0 {
		optCommitNow = commitNow[0]
	}

	q := &Query{ctx: t.ctx, tx: t.txn, model: model}
	return &Deleter{q: q, ctx: t.ctx, tx: t.txn, commitNow: optCommitNow}
}

// Get prepares a query for a model
func (t *TxnContext) Get(model interface{}) *Query {
	return &Query{ctx: t.ctx, tx: t.txn, model: model}
}

// NewTxnContext creates a new transaction coupled with a context
func NewTxnContext(ctx context.Context, c *dgo.Dgraph) *TxnContext {
	return &TxnContext{
		txn: c.NewTxn(),
		ctx: ctx,
	}
}

// NewTxn creates a new transaction
func NewTxn(c *dgo.Dgraph) *TxnContext {
	return NewTxnContext(context.Background(), c)
}

// NewReadOnlyTxnContext creates a new read only transaction coupled with a context
func NewReadOnlyTxnContext(ctx context.Context, c *dgo.Dgraph) *TxnContext {
	return &TxnContext{
		txn: c.NewReadOnlyTxn(),
		ctx: ctx,
	}
}

// NewReadOnlyTxn creates a new read only transaction
func NewReadOnlyTxn(c *dgo.Dgraph) *TxnContext {
	return NewReadOnlyTxnContext(context.Background(), c)
}
