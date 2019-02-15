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
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dgraph-io/dgo/protos/api"

	"github.com/dgraph-io/dgo"
)

var (
	ErrNodeNotFound = errors.New("node not found")
)

// Node marshals a single node to a single object of model,
// returns error if no nodes are found,
// query root must be data(func ...)
func Node(jsonData []byte, model interface{}) error {
	var result struct {
		Data []json.RawMessage
	}

	if err := json.Unmarshal(jsonData, &result); err != nil {
		return err
	}

	if len(result.Data) == 0 {
		return ErrNodeNotFound
	}

	val := result.Data[0]
	if err := json.Unmarshal(val, model); err != nil {
		return err
	}

	return nil
}

// Nodes marshals multiple nodes to a slice of model,
// query root must be data(func ...)
func Nodes(jsonData []byte, model interface{}) error {
	var result struct {
		Data json.RawMessage
	}

	if err := json.Unmarshal(jsonData, &result); err != nil {
		return err
	}

	if err := json.Unmarshal(result.Data, model); err != nil {
		return err
	}

	return nil
}

type Query struct {
	ctx         context.Context
	tx          *dgo.Txn
	model       interface{}
	queryString string
	paramString string
	vars        map[string]string
	first       int
	offset      int
	after       string
}

func (q *Query) Query(query string) *Query {
	q.queryString = query
	return q
}

func (q *Query) Filter(filter string) *Query {
	q.queryString = fmt.Sprintf(`@filter(%s) {
		uid
		expand(_all_)
	}`, filter)
	return q
}

func (q *Query) UID(uid string) error {
	query := fmt.Sprintf(`{
		data(func: uid(%s)) {
			uid
			expand(_all_)
		}
	}`, uid)

	resp, err := q.tx.Query(q.ctx, query)
	if err != nil {
		return err
	}

	return Node(resp.Json, q.model)
}

func (q *Query) Vars(funcDef string, vars map[string]string) *Query {
	q.paramString = funcDef
	q.vars = vars
	return q
}

func (q *Query) First(n int) *Query {
	q.first = n
	return q
}

func (q *Query) Offset(n int) *Query {
	q.offset = n
	return q
}

func (q *Query) After(uid string) *Query {
	q.after = uid
	return q
}

func (q *Query) Node() (err error) {
	result, err := q.executeQuery()
	if err != nil {
		return err
	}
	return Node(result, q.model)
}

func (q *Query) Nodes() error {
	result, err := q.executeQuery()
	if err != nil {
		return err
	}
	return Nodes(result, q.model)
}

func (q *Query) String() string {
	query := ""
	if q.vars != nil {
		query = "query " + q.paramString
	}

	nodeType := GetNodeType(q.model)
	query += fmt.Sprintf(`{
	data(func: has(%s)`, nodeType)

	if q.first != 0 {
		query += fmt.Sprintf(", first: %d", q.first)
	}

	if q.offset != 0 {
		query += fmt.Sprintf(", offset: %d", q.offset)
	}

	if q.after != "" {
		query += fmt.Sprintf(", after: %s", q.after)
	}

	query += fmt.Sprintf(`) %s
}`, q.queryString)

	return query
}

func (q *Query) executeQuery() (result []byte, err error) {
	queryString := q.String()

	var resp *api.Response
	if q.vars != nil {
		resp, err = q.tx.QueryWithVars(q.ctx, queryString, q.vars)
	} else {
		resp, err = q.tx.Query(q.ctx, queryString)
	}
	if err != nil {
		return nil, err
	}

	return resp.Json, nil
}

// Get
func Get(ctx context.Context, tx *dgo.Txn, model interface{}) *Query {
	return &Query{ctx: ctx, tx: tx, model: model}
}
