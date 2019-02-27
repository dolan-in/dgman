/*
 * Copyright (C) 2018 Dolan and Contributors
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
	"bytes"
	"context"

	"github.com/dgraph-io/dgo/protos/api"

	"github.com/dgraph-io/dgo"
)

type Deleter struct {
	ctx       context.Context
	tx        *dgo.Txn
	model     interface{}
	q         *Query
	mutateOpt MutateOptions
}

func (d *Deleter) Query(query string) *Deleter {
	d.q.queryString = query
	return d
}

func (d *Deleter) Filter(filter string) *Deleter {
	d.q.queryString = `@filter(` + filter + `) {
		uid
	}`
	return d
}

// All returns all nodes of the specified node type (from model)
func (d *Deleter) All() *Deleter {
	d.q.queryString = `{
		uid
	}`
	return d
}

// Vars specify the GraphQL variables to be passed on the query,
// by specifying the function definition of vars, and variable map.
// Example funcDef: getUserByEmail($email: string, $age: number)
func (d *Deleter) Vars(funcDef string, vars map[string]string) *Deleter {
	d.q.paramString = funcDef
	d.q.vars = vars
	return d
}

// RootFunc modifies the dgraph query root function, if not set,
// the default is "has(node_type)"
func (d *Deleter) RootFunc(rootFunc string) *Deleter {
	d.q.rootFunc = rootFunc
	return d
}

func (d *Deleter) First(n int) *Deleter {
	d.q.first = n
	return d
}

func (d *Deleter) Offset(n int) *Deleter {
	d.q.offset = n
	return d
}

func (d *Deleter) After(uid string) *Deleter {
	d.q.after = uid
	return d
}

func (d *Deleter) OrderAsc(clause string) *Deleter {
	d.q.order = append(d.q.order, order{clause: clause})
	return d
}

func (d *Deleter) OrderDesc(clause string) *Deleter {
	d.q.order = append(d.q.order, order{descending: true, clause: clause})
	return d
}

// Node deletes the first single root node from the query
// including edge nodes that may be specified on the query
func (d *Deleter) Node() (uids []string, err error) {
	result, err := d.q.executeQuery()
	if err != nil {
		return nil, err
	}

	model := make(map[string]interface{})
	if err := Node(result, &model); err != nil {
		return nil, err
	}

	traverseUIDs(&uids, model)

	return uids, d.deleteUids(uids)
}

// Nodes deletes all nodes matching the delete query
// including edge nodes that may be specified on the query
func (d *Deleter) Nodes() (uids []string, err error) {
	result, err := d.q.executeQuery()
	if err != nil {
		return nil, err
	}

	var model []map[string]interface{}
	if err := Nodes(result, &model); err != nil {
		return nil, err
	}

	for _, m := range model {
		traverseUIDs(&uids, m)
	}

	return uids, d.deleteUids(uids)
}

func (d *Deleter) deleteUids(uids []string) error {
	uidsJSON := generateUidsJSON(uids)
	_, err := d.tx.Mutate(d.ctx, &api.Mutation{
		DeleteJson: uidsJSON,
		CommitNow:  d.mutateOpt.CommitNow,
	})

	return err
}

func generateUidsJSON(uids []string) []byte {
	var b bytes.Buffer

	b.WriteByte('[')
	for i, uid := range uids {
		b.WriteString(`{"uid":"`)
		b.WriteString(uid)
		b.WriteString(`"}`)

		if i != len(uids)-1 {
			b.WriteByte(',')
		}
	}
	b.WriteByte(']')
	return b.Bytes()
}

func traverseUIDs(uids *[]string, model map[string]interface{}) {
	for predicate, val := range model {
		switch v := val.(type) {
		case []interface{}:
			for _, node := range v {
				if v2, ok := node.(map[string]interface{}); ok {
					traverseUIDs(uids, v2)
				}
			}
		case string:
			if predicate == "uid" {
				*uids = append(*uids, val.(string))
			}
		}
	}
}

func (d *Deleter) String() string {
	return d.q.String()
}

// Delete prepares a delete mutation using a query
func Delete(ctx context.Context, tx *dgo.Txn, model interface{}, opt ...MutateOptions) *Deleter {
	mutateOpt := MutateOptions{}
	if len(opt) > 0 {
		mutateOpt = opt[0]
	}

	q := &Query{ctx: ctx, tx: tx, model: model}
	return &Deleter{q: q, ctx: ctx, tx: tx, mutateOpt: mutateOpt}
}
