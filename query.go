/*
 * Copyright (C) 2018-2020 Dolan and Contributors
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
	"strconv"
	"strings"

	"github.com/dgraph-io/dgo/v200/protos/api"

	"github.com/dgraph-io/dgo/v200"
)

var (
	ErrNodeNotFound = errors.New("node not found")
)

// ParamFormatter provides an interface for types to implement custom
// parameter formatter for query parameters
type ParamFormatter interface {
	FormatParams() []byte
}

type QueryBlock struct {
	ctx         context.Context
	tx          *dgo.Txn
	paramString string
	vars        map[string]string
	blocks      []*Query
}

// Vars specify the GraphQL variables to be passed on the query,
// by specifying the function definition of vars, and variable map.
// Example funcDef: getUserByEmail($email: string)
func (q *QueryBlock) Vars(funcDef string, vars map[string]string) *QueryBlock {
	q.paramString = funcDef
	q.vars = vars
	return q
}

// Add adds a query to the query block
func (q *QueryBlock) Add(query *Query) *QueryBlock {
	q.blocks = append(q.blocks, query)
	return q
}

// Blocks set the query blocks
func (q *QueryBlock) Blocks(query ...*Query) *QueryBlock {
	q.blocks = query
	return q
}

// Scan unmarshals the query result into provided destination
func (q *QueryBlock) Scan(dst interface{}) error {
	result, err := q.executeQuery()
	if err != nil {
		return err
	}
	return json.Unmarshal(result, dst)
}

func (q *QueryBlock) String() string {
	var queryBuf strings.Builder
	if q.vars != nil {
		queryBuf.WriteString("query ")
		queryBuf.WriteString(q.paramString)
	}

	queryBuf.WriteString("{\n")

	for _, block := range q.blocks {
		block.generateQuery(&queryBuf)
	}

	queryBuf.WriteString("}")

	return queryBuf.String()
}

func (q *QueryBlock) executeQuery() (result []byte, err error) {
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

type order struct {
	descending bool
	clause     string
}

type Query struct {
	ctx         context.Context
	tx          *dgo.Txn
	model       interface{}
	name        string
	as          string
	isVar       bool
	paramString string
	vars        map[string]string
	rootFunc    string
	first       int
	offset      int
	after       string
	order       []order
	groupBy     string
	uid         string
	filter      string
	query       string
}

type PagedResults struct {
	Result   json.RawMessage
	PageInfo []*PageInfo
}

type PageInfo struct {
	Count int
}

// Name defines the query block name, which identifies the query results
func (q *Query) Name(queryName string) *Query {
	q.name = queryName
	return q
}

// As defines a query variable name
// https://dgraph.io/docs/query-language/#query-variables
func (q *Query) As(varName string) *Query {
	q.as = varName
	return q
}

// Var defines whether a query block is a var, which are not returned in query results
func (q *Query) Var() *Query {
	q.isVar = true
	return q
}

// Type sets the model struct to infer the node type
func (q *Query) Type(model interface{}) *Query {
	q.model = model
	return q
}

// Query defines the query portion other than the root function
func (q *Query) Query(query string, params ...interface{}) *Query {
	q.query = parseQueryWithParams(query, params)
	return q
}

// Filter defines a query filter, return predicates at the first depth
func (q *Query) Filter(filter string, params ...interface{}) *Query {
	q.filter = parseQueryWithParams(filter, params)
	return q
}

// UID returns the node with the specified uid
func (q *Query) UID(uid string) *Query {
	q.uid = uid
	return q
}

func expandPredicate(depth int) string {
	var buffer strings.Builder

	buffer.WriteString("{\n\t\tuid\n\t\tdgraph.type\n\t\texpand(_all_)")
	for i := 0; i < depth; i++ {
		tabs := strings.Repeat("\t", i+1)
		buffer.WriteString(" {\n\t\t")
		buffer.WriteString(tabs)
		buffer.WriteString("uid\n\t\t")
		buffer.WriteString(tabs)
		buffer.WriteString("dgraph.type\n\t\t")
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
// the default is "type(NodeType)"
func (q *Query) RootFunc(rootFunc string) *Query {
	q.rootFunc = rootFunc
	return q
}

// First returns n number of results
func (q *Query) First(n int) *Query {
	q.first = n
	return q
}

// Offset skips n number of results
func (q *Query) Offset(n int) *Query {
	q.offset = n
	return q
}

// After uses default UID ordering to skip directly past a node specified by UID
func (q *Query) After(uid string) *Query {
	q.after = uid
	return q
}

// OrderAsc adds an ascending order clause
func (q *Query) OrderAsc(clause string) *Query {
	q.order = append(q.order, order{clause: clause})
	return q
}

// OrderDesc adds an descending order clause
func (q *Query) OrderDesc(clause string) *Query {
	q.order = append(q.order, order{descending: true, clause: clause})
	return q
}

// GroupBy defines the predicate to group the query by
func (q *Query) GroupBy(predicate string) *Query {
	q.groupBy = predicate
	return q
}

// Node returns the first single node from the query,
// optional destination can be passed, otherwise bind to model
func (q *Query) Node(dst ...interface{}) (err error) {
	model := q.model
	if len(dst) > 0 {
		model = dst[0]
	}

	// make sure only 1 node is return
	q.first = 1

	result, err := q.executeQuery()
	if err != nil {
		return err
	}

	return q.node(result, model)
}

func (q *Query) node(jsonData []byte, dst interface{}) error {
	dataLen := len(jsonData)
	// JSON data must be in format {"<name>":[{ ... }]}
	// get only inner object
	dataPrefixLen := len(fmt.Sprintf(`{"%s":[`, q.name))
	if dataLen < dataPrefixLen {
		return fmt.Errorf("invalid json result for node: %s", jsonData)
	}

	// remove prefix and the ending array closer ']'
	dataBytes := jsonData[dataPrefixLen : dataLen-2]

	if len(dataBytes) == 0 {
		return ErrNodeNotFound
	}

	return json.Unmarshal(dataBytes, dst)
}

// Nodes returns all results from the query,
// optional destination can be passed, otherwise bind to model
func (q *Query) Nodes(dst ...interface{}) error {
	model := q.model
	if len(dst) > 0 {
		model = dst[0]
	}

	result, err := q.executeQuery()
	if err != nil {
		return err
	}
	return q.nodes(result, model)
}

func (q *Query) nodes(jsonData []byte, dst interface{}) error {
	dataLen := len(jsonData)
	// JSON data must start with {"data":
	dataPrefixLen := len(fmt.Sprintf(`{"%s":`, q.name))
	if dataLen < dataPrefixLen {
		return fmt.Errorf("invalid json result for nodes: %s", jsonData)
	}

	dataBytes := jsonData[dataPrefixLen : dataLen-1]

	return json.Unmarshal(dataBytes, dst)
}

// NodesAndCount return paged nodes result with the total count of the query
func (q *Query) NodesAndCount() (count int, err error) {
	tx := TxnContext{txn: q.tx, ctx: q.ctx}

	pagedResult := PagedResults{}
	query := tx.Query(
		NewQuery().
			As("filtered").
			Var().
			UID(q.uid).
			RootFunc(q.rootFunc).
			Type(q.model).
			Filter(q.filter),
		NewQuery().
			Name("result").
			UID("filtered").
			First(q.first).
			After(q.after).
			Offset(q.offset),
		NewQuery().
			Name("pageInfo").
			UID("filtered").
			Query(`{ count(uid) }`),
	)

	err = query.Scan(&pagedResult)
	if err != nil {
		return 0, err
	}

	if pagedResult.Result == nil {
		return 0, nil
	}

	if err := json.Unmarshal(pagedResult.Result, q.model); err != nil {
		return 0, err
	}

	return pagedResult.PageInfo[0].Count, nil
}

func (q *Query) generateQuery(queryBuf *strings.Builder) {
	queryBuf.WriteString("\t")

	if q.as != "" {
		queryBuf.WriteString(q.as)
		queryBuf.WriteString(" as ")
	}

	if q.isVar {
		queryBuf.WriteString("var")
	} else {
		queryBuf.WriteString(q.name)
	}

	// START ROOT FUNCTION
	queryBuf.WriteString("(func: ")

	if q.uid != "" {
		queryBuf.WriteString("uid(")
		queryBuf.WriteString(q.uid)
		queryBuf.WriteString(")")
	} else if q.rootFunc != "" {
		queryBuf.WriteString(q.rootFunc)
	} else {
		// if root function is not defined, query from node type
		nodeType := GetNodeType(q.model)
		queryBuf.WriteString("type(")
		queryBuf.WriteString(nodeType)
		queryBuf.WriteByte(')')
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
	queryBuf.WriteString(") ")
	// END ROOT FUNCTION

	if q.filter != "" {
		queryBuf.WriteString("@filter(")
		queryBuf.WriteString(q.filter)
		queryBuf.WriteString(") ")
	}

	if q.groupBy != "" {
		queryBuf.WriteString("@groupby(")
		queryBuf.WriteString(q.groupBy)
		queryBuf.WriteString(") ")
	}

	// allow var to have empty query block
	if !q.isVar {
		if q.query == "" {
			q.All()
		}
	}

	queryBuf.WriteString(q.query)
	queryBuf.WriteString("\n")
}

func (q *Query) String() string {
	var queryBuf strings.Builder
	if q.vars != nil {
		queryBuf.WriteString("query ")
		queryBuf.WriteString(q.paramString)
	}

	queryBuf.WriteString("{\n")

	q.generateQuery(&queryBuf)

	queryBuf.WriteString("}")

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

// NewQueryBlock returns a new empty query block
func NewQueryBlock() *Query {
	return &Query{}
}

// NewQuery returns a new empty query
func NewQuery() *Query {
	return &Query{}
}

func parseQueryWithParams(query string, params []interface{}) string {
	var buffer strings.Builder
	queryLength := len(query)
	paramsLength := len(params)
	for pos := 0; pos < queryLength; pos++ {
		// try to parse param index, to retrieve params
		if query[pos] == '$' {
			// skip if next rune is out of bounds
			pos++
			if pos > queryLength-1 {
				break
			}

			var numStr []byte
			for ; query[pos] >= '0' && query[pos] <= '9'; pos++ {
				numStr = append(numStr, query[pos])
			}

			if numStr == nil {
				// probably a GraphQL named var, go backwards and include the $
				pos--
				goto write
			}

			paramIndex, err := strconv.Atoi(string(numStr))
			if err != nil {
				goto write
			}

			// paramIndex starts from 1
			if paramIndex > paramsLength {
				goto write
			}

			var paramString []byte
			param := params[paramIndex-1]
			if formatter, ok := param.(ParamFormatter); ok {
				paramString = formatter.FormatParams()
			} else {
				paramString, err = json.Marshal(param)
				if err != nil {
					goto write
				}
			}

			buffer.Write(paramString)
		}
	write:
		buffer.WriteByte(query[pos])
	}
	return buffer.String()
}
