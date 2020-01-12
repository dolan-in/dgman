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
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/dgraph-io/dgo/v2/protos/api"

	"github.com/dgraph-io/dgo/v2"
)

var (
	ErrNodeNotFound = errors.New("node not found")
)

type order struct {
	descending bool
	clause     string
}

type Query struct {
	ctx         context.Context
	tx          *dgo.Txn
	model       interface{}
	paramString string
	vars        map[string]string
	rootFunc    string
	first       int
	offset      int
	after       string
	order       []order
	uid         string
	filter      string
	query       string
}

// Query defines the query portion other than the root function
func (q *Query) Query(query string) *Query {
	q.query = query
	return q
}

// Filter defines a query filter, return predicates at the first depth
func (q *Query) Filter(filter string) *Query {
	q.filter = filter
	return q
}

// UID returns the node with the specified uid
func (q *Query) UID(uid string) *Query {
	q.uid = uid
	return q
}

func expandPredicate(depth int) string {
	var buffer bytes.Buffer

	buffer.WriteString("{\n\t\tuid\n\t\texpand(_all_)")
	for i := 0; i < depth; i++ {
		tabs := strings.Repeat("\t", i+1)
		buffer.WriteString(" {\n\t\t")
		buffer.WriteString(tabs)
		buffer.WriteString("uid\n\t\t")
		buffer.WriteString(tabs)
		buffer.WriteString("expand(_all_)")
	}
	for i := depth - 1; i >= 0; i-- {
		tabs := strings.Repeat("\t", i)
		buffer.WriteString("\n\t\t")
		buffer.WriteString(tabs)
		buffer.WriteString("}")
	}
	buffer.WriteString("\n\t}")

	return buffer.String()
}

// All returns expands all predicates, with a depth parameter that specifies
// how deep should edges be expanded
func (q *Query) All(depthParam ...int) *Query {
	depth := 0
	if len(depthParam) > 0 {
		depth = depthParam[0]
	}

	q.query = expandPredicate(depth)
	return q
}

// Vars specify the GraphQL variables to be passed on the query,
// by specifying the function definition of vars, and variable map.
// Example funcDef: getUserByEmail($email: string)
func (q *Query) Vars(funcDef string, vars map[string]string) *Query {
	q.paramString = funcDef
	q.vars = vars
	return q
}

// RootFunc modifies the dgraph query root function, if not set,
// the default is "has(node_type)"
func (q *Query) RootFunc(rootFunc string) *Query {
	q.rootFunc = rootFunc
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

func (q *Query) OrderAsc(clause string) *Query {
	q.order = append(q.order, order{clause: clause})
	return q
}

func (q *Query) OrderDesc(clause string) *Query {
	q.order = append(q.order, order{descending: true, clause: clause})
	return q
}

// Node returns the first single node from the query
func (q *Query) Node() (err error) {
	// make sure only 1 node is return
	q.first = 1

	result, err := q.executeQuery()
	if err != nil {
		return err
	}
	return Node(result, q.model)
}

// Nodes returns all results from the query
func (q *Query) Nodes() error {
	result, err := q.executeQuery()
	if err != nil {
		return err
	}
	return Nodes(result, q.model)
}

func (q *Query) String() string {
	var queryBuf bytes.Buffer
	if q.vars != nil {
		queryBuf.WriteString("query ")
		queryBuf.WriteString(q.paramString)
	}

	// START ROOT FUNCTION
	queryBuf.WriteString("{\n\tdata(func: ")

	if q.uid != "" {
		queryBuf.WriteString("uid(")
		queryBuf.WriteString(q.uid)
		queryBuf.WriteString(")")
	} else {
		if q.rootFunc == "" {
			// if root function is not defined, query from node type
			nodeType := GetNodeType(q.model)
			queryBuf.WriteString("type(")
			queryBuf.WriteString(nodeType)
			queryBuf.WriteByte(')')
		} else {
			queryBuf.WriteString(q.rootFunc)
		}

		if q.first != 0 {
			queryBuf.WriteString(", first: ")
			queryBuf.Write(intToBytes(q.first))
		}

		if q.offset != 0 {
			queryBuf.WriteString(", offset: ")
			queryBuf.Write(intToBytes(q.offset))
		}

		if q.after != "" {
			queryBuf.WriteString(", after: ")
			queryBuf.WriteString(q.after)
		}

		if len(q.order) > 0 {
			for _, order := range q.order {
				orderStr := ", orderasc: "
				if order.descending {
					orderStr = ", orderdesc: "
				}
				queryBuf.WriteString(orderStr)
				queryBuf.WriteString(order.clause)
			}
		}
	}
	queryBuf.WriteString(") ")
	// END ROOT FUNCTION

	if q.filter != "" {
		queryBuf.WriteString("@filter(")
		queryBuf.WriteString(q.filter)
		queryBuf.WriteByte(')')
	}

	if q.query == "" {
		q.All()
	}

	queryBuf.WriteString(q.query)
	queryBuf.WriteString(" \n}")

	return queryBuf.String()
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

// Get prepares a query for a model
func Get(ctx context.Context, tx *dgo.Txn, model interface{}) *Query {
	return &Query{ctx: ctx, tx: tx, model: model}
}

// Node marshals a single node to a single object of model,
// returns error if no nodes are found,
// query root must be data(func ...)
func Node(jsonData []byte, model interface{}) error {
	// JSON data must be in format {"data":[{ ... }]}
	// get only inner object
	dataPrefix := `{"data":[`
	strippedPrefix := strings.TrimPrefix(string(jsonData), dataPrefix)

	if len(strippedPrefix) == len(jsonData)-len(dataPrefix) {
		dataBytes := []byte(strippedPrefix)
		// remove the ending array closer ']'
		dataBytes = dataBytes[:len(dataBytes)-2]

		if len(dataBytes) == 0 {
			return ErrNodeNotFound
		}

		return json.Unmarshal(dataBytes, model)
	}

	return fmt.Errorf("invalid json result for node: %s", jsonData)
}

// Nodes marshals multiple nodes to a slice of model,
// query root must be data(func ...)
func Nodes(jsonData []byte, model interface{}) error {
	// JSON data must start with {"data":
	dataPrefix := `{"data":`
	strippedPrefix := strings.TrimPrefix(string(jsonData), dataPrefix)

	if len(strippedPrefix) == len(jsonData)-len(dataPrefix) {
		dataBytes := []byte(strippedPrefix)
		return json.Unmarshal(dataBytes[:len(dataBytes)-1], model)
	}

	return fmt.Errorf("invalid json result for nodes: %s", jsonData)
}
