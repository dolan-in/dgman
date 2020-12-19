/*
 * Copyright (C) 2019 Dolan and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dgman

import (
	"context"

	"github.com/dgraph-io/dgo/v200"
	"github.com/pkg/errors"
)

// TxnContext is dgo transaction coupled with context
type TxnContext struct {
	txn       *dgo.Txn
	ctx       context.Context
	commitNow bool
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

// CommitNow specifies whether to commit as soon as a mutation is called,
//
// i.e: set CommitNow: true in dgo.api.Mutation.
//
// If this is called, a transaction can only be used for a single mutation.
func (t *TxnContext) CommitNow() *TxnContext {
	t.commitNow = true
	return t
}

// Mutate does a dgraph mutation, with recursive automatic uid injection (on empty uid fields),
// type injection (using the dgraph.type field), unique checking on fields (if applicable), and returns the created uids.
// It will return a UniqueError when unique checking fails on a field.
func (t *TxnContext) Mutate(data interface{}) ([]string, error) {
	return newMutation(t, data).do()
}

// MutateBasic does a dgraph mutation like Mutate, but without any unique checking.
// This should be quite faster if there is no uniqueness requirement on the node type
func (t *TxnContext) MutateBasic(data interface{}) ([]string, error) {
	return newMutation(t, data).mutate()
}

// MutateOrGet does a dgraph mutation like Mutate, but instead of returning a UniqueError when a node already exists
// for a predicate value, it will get the existing node and inject it into the struct values.
// Optionally, a list of predicates can be passed to be specify predicates to be unique checked.
// A single node type can only have a single upsert predicate.
func (t *TxnContext) MutateOrGet(data interface{}, predicates ...string) ([]string, error) {
	mutation := newMutation(t, data)
	mutation.opcode = mutationMutateOrGet
	mutation.upsertFields = newSet(predicates...)
	return mutation.do()
}

// Upsert does a dgraph mutation like Mutate, but instead of returning a UniqueError when a node already exists
// for a predicate value, it will update the existing node and inject it into the struct values.
// Optionally, a list of predicates can be passed to be specify predicates to be unique checked.
// A single node type can only have a single upsert predicate.
func (t *TxnContext) Upsert(data interface{}, predicates ...string) ([]string, error) {
	mutation := newMutation(t, data)
	mutation.opcode = mutationUpsert
	mutation.upsertFields = newSet(predicates...)
	return mutation.do()
}

// Delete will delete nodes using delete parameters, which will generate RDF n-quads for deleting
func (t *TxnContext) Delete(params ...*DeleteParams) error {
	if len(params) == 0 {
		return errors.New("params cannot be empty")
	}
	return t.delete(params...)
}

// DeleteQuery will delete nodes using a query and delete parameters, which will generate RDF n-quads for deleting
// based on the query
func (t *TxnContext) DeleteQuery(query *QueryBlock, params ...*DeleteParams) (DeleteQuery, error) {
	if len(params) == 0 {
		return DeleteQuery{}, errors.New("conds cannot be empty")
	}
	return t.deleteQuery(query, params...)
}

// DeleteNode will delete a node(s) by its explicit uid
func (t *TxnContext) DeleteNode(uids ...string) error {
	if len(uids) == 0 {
		return errors.New("uids cannot be empty")
	}
	return t.deleteNode(uids...)
}

// DeleteEdge will delete an edge of a node by predicate, optionally you can pass which edge uids to delete,
// if none are passed, all edges of that predicate will be deleted
func (t *TxnContext) DeleteEdge(uid string, predicate string, uids ...string) error {
	return t.deleteEdge(uid, predicate, uids...)
}

// Get prepares a query for a model
func (t *TxnContext) Get(model interface{}) *Query {
	return &Query{ctx: t.ctx, tx: t.txn, model: model, name: "data"}
}

// Query prepares a query with multiple query block
func (t *TxnContext) Query(query ...*Query) *QueryBlock {
	return &QueryBlock{ctx: t.ctx, tx: t.txn, blocks: query}
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
