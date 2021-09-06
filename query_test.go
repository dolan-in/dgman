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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestModel struct {
	UID     string     `json:"uid"`
	Name    string     `json:"name" dgraph:"index=term"`
	Address string     `json:"address,omitempty"`
	Age     int        `json:"age" dgraph:"index=int"`
	Dead    bool       `json:"dead"`
	Edges   []TestEdge `json:"edges,omitempty"`
	DType   []string   `json:"dgraph.type,omitempty"`
}

type TestEdge struct {
	UID   string   `json:"uid"`
	Level string   `json:"level,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

func TestGetByUID(t *testing.T) {
	source := &TestModel{
		Name:    "wildanjing",
		Address: "Beverly Hills",
		Age:     17,
	}

	c := newDgraphClient()
	defer dropAll(c)

	_, err := CreateSchema(c, &TestModel{})
	if err != nil {
		t.Error(err)
	}

	tx := NewTxn(c).SetCommitNow()

	_, err = tx.Mutate(source)
	if err != nil {
		t.Error(err)
	}

	dst := &TestModel{}
	tx = NewTxn(c)
	if err := tx.Get(dst).UID(source.UID).Node(); err != nil {
		t.Error(err)
	}

	assert.Equal(t, source.Name, dst.Name)
	assert.Equal(t, source.Address, dst.Address)
	assert.Equal(t, source.Age, dst.Age)
	assert.Equal(t, source.Dead, dst.Dead)
}

func TestGetByFilter(t *testing.T) {
	source := &TestModel{
		Name:    "wildan anjing",
		Address: "Beverly Hills",
		Age:     17,
	}

	c := newDgraphClient()
	if _, err := CreateSchema(c, source); err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	tx := NewTxn(c).SetCommitNow()

	_, err := tx.Mutate(source)
	if err != nil {
		t.Error(err)
	}

	dst := &TestModel{}
	tx = NewTxn(c)
	if err := tx.Get(dst).Filter(`allofterms(name, "wildan")`).Node(); err != nil {
		t.Error(err)
	}

	assert.Equal(t, source.Name, dst.Name)
	assert.Equal(t, source.Address, dst.Address)
	assert.Equal(t, source.Age, dst.Age)
	assert.Equal(t, source.Dead, dst.Dead)

	dst = &TestModel{}
	tx = NewTxn(c)
	if err := tx.Get(dst).Filter(`allofterms(name, "onono")`).Node(); err != ErrNodeNotFound {
		t.Error(err)
	}
}

func TestCascade(t *testing.T) {
	source := []TestModel{
		{
			Name: "wildan anjing",
			Age:  17,
		},
		{
			Name:    "moh wildan",
			Address: "Beverly Hills",
			Edges: []TestEdge{
				{
					Level: "1",
				},
			},
			Age: 17,
		},
		{
			Name: "2moh wildan",
			Edges: []TestEdge{
				{
					Level: "1",
				},
			},
			Age: 17,
		},
		{
			Name:    "wildancok",
			Address: "Beverly Hills",
			Age:     17,
		},
	}

	c := newDgraphClient()
	if _, err := CreateSchema(c, &TestModel{}); err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	tx := NewTxn(c)

	_, err := tx.Mutate(&source)
	if err != nil {
		t.Error(err)
	}
	tx.Commit()

	tests := []struct {
		name          string
		applyCascade  func(q *Query) *Query
		expectedCount int
	}{
		{
			name: "Cascade all fields",
			applyCascade: func(q *Query) *Query {
				return q.Cascade()
			},
			expectedCount: 1,
		},
		{
			name: "Cascade single field",
			applyCascade: func(q *Query) *Query {
				return q.Cascade("edges")
			},
			expectedCount: 2,
		},
		{
			name: "Cascade multiple fields",
			applyCascade: func(q *Query) *Query {
				return q.Cascade("address", "edges")
			},
			expectedCount: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var dst []TestModel
			tx = NewTxn(c)
			if err := test.applyCascade(tx.Get(&dst).Filter("allofterms(name, $1)", "wildan")).Query(`{
		 uid
		 expand(_all_)
	 }`).Nodes(); err != nil {
				t.Error(err)
			}
			assert.Len(t, dst, test.expectedCount)
		})
	}

}

func TestFind(t *testing.T) {
	source := []TestModel{
		{
			Name:    "wildan anjing",
			Address: "Beverly Hills",
			Age:     17,
		},
		{
			Name:    "moh wildan",
			Address: "Beverly Hills",
			Age:     17,
		},
		{
			Name:    "wildancok",
			Address: "Beverly Hills",
			Age:     17,
		},
	}

	c := newDgraphClient()
	if _, err := CreateSchema(c, &TestModel{}); err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	tx := NewTxn(c)

	_, err := tx.Mutate(&source)
	if err != nil {
		t.Error(err)
	}
	tx.Commit()

	var dst []TestModel
	tx = NewTxn(c)
	if err := tx.Get(&dst).Filter("allofterms(name, $1)", "wildan").Query(`{
		 uid
		 expand(_all_)
	 }`).Nodes(); err != nil {
		t.Error(err)
	}

	assert.Len(t, dst, 2)
}

func TestGetByQuery(t *testing.T) {
	source := TestModel{
		Name:    "wildan anjing",
		Address: "Beverly Hills",
		Age:     17,
	}

	c := newDgraphClient()
	if _, err := CreateSchema(c, &TestModel{}); err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	tx := NewTxn(c)

	_, err := tx.Mutate(&source)
	if err != nil {
		t.Error(err)
	}
	tx.Commit()

	source2 := TestEdge{
		Level: "one",
	}

	tx = NewTxn(c)
	_, err = tx.Mutate(&source2)
	if err != nil {
		t.Error(err)
	}
	tx.Commit()

	source.Edges = []TestEdge{source2}

	tx = NewTxn(c)
	_, err = tx.Mutate(&source)
	if err != nil {
		t.Error(err)
	}
	tx.Commit()

	var dst TestModel
	tx = NewTxn(c)
	q := tx.Get(&dst).
		Filter(`allofterms(name, "wildan")`).
		All(2)
	if err := q.Node(); err != nil {
		t.Error(err)
	}

	assert.Equal(t, dst.UID, source.UID)
	assert.Len(t, dst.Edges, 1)
	assert.Equal(t, dst.Edges[0].UID, source2.UID)
	assert.Equal(t, dst.Edges[0].Level, source2.Level)
}

func TestGetAllWithDepth(t *testing.T) {
	source := TestModel{
		Name:    "wildan anjing",
		Address: "Beverly Hills",
		Age:     17,
		Edges: []TestEdge{{
			Level: "one",
		}},
	}

	c := newDgraphClient()
	if _, err := CreateSchema(c, &TestModel{}); err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	tx := NewTxn(c).SetCommitNow()

	uids, err := tx.Mutate(&source)
	if err != nil {
		t.Error(err)
	}
	assert.Len(t, uids, 2)

	var dst TestModel
	tx = NewTxn(c)
	q := tx.Get(&dst).
		Filter(`allofterms(name, "wildan")`).
		All(2)
	if err := q.Node(); err != nil {
		t.Error(err)
	}

	assert.Equal(t, source.UID, dst.UID)
	assert.Len(t, dst.Edges, 1)
	assert.Equal(t, source.Edges[0].UID, dst.Edges[0].UID)
	assert.Equal(t, source.Edges[0].Level, dst.Edges[0].Level)
}

func TestPagination(t *testing.T) {
	models := []TestModel{}
	for i := 0; i < 10; i++ {
		models = append(models, TestModel{
			Name:    fmt.Sprintf("wildan %d", i),
			Address: "Beverly Hills",
			Age:     17,
		})
	}
	c := newDgraphClient()
	if _, err := CreateSchema(c, &TestModel{}); err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	tx := NewTxn(c)
	_, err := tx.Mutate(&models)
	if err != nil {
		t.Error(err)
	}
	tx.Commit()

	result := []TestModel{}
	query := NewReadOnlyTxn(c).Get(&result).
		Vars("getWithNames($name: string)", map[string]string{"$name": "wildan"}).
		Filter("allofterms(name, $name)").
		First(5)
	if err = query.Nodes(); err != nil {
		t.Error(err)
	}

	assert.Len(t, result, 5)

	result = []TestModel{}
	query = NewReadOnlyTxn(c).Get(&result).
		Vars("getWithNames($name: string)", map[string]string{"$name": "nonon"}).
		Filter("allofterms(name, $name)").
		First(5)
	if err = query.Nodes(); err != nil {
		t.Error(err)
	}

	assert.Len(t, result, 0)

	result = []TestModel{}
	query = NewReadOnlyTxn(c).Get(&result).
		Vars("getWithNames($name: string)", map[string]string{"$name": "wildan"}).
		Filter("allofterms(name, $name)").
		First(255555)
	if err = query.Nodes(); err != nil {
		t.Error(err)
	}

	assert.Len(t, result, 10)
}

func TestOrder(t *testing.T) {
	models := []*TestModel{}
	for i := 0; i < 10; i++ {
		models = append(models, &TestModel{
			Name:    fmt.Sprintf("wildan %d", i%10),
			Address: "Beverly Hills",
			Age:     i,
		})
	}
	c := newDgraphClient()
	if _, err := CreateSchema(c, &TestModel{}); err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	tx := NewTxn(c)

	_, err := tx.Mutate(&models)
	if err != nil {
		t.Error(err)
	}
	tx.Commit()

	result := []*TestModel{}
	query := NewReadOnlyTxn(c).Get(&result).
		Vars("getWithNames($name: string)", map[string]string{"$name": "wildan"}).
		Filter("allofterms(name, $name)").
		OrderAsc("age")
	if err = query.Nodes(); err != nil {
		t.Error(err)
	}

	assert.Len(t, result, 10)

	for i, r := range result {
		assert.Equal(t, models[i], r)
	}

	result = []*TestModel{}
	query = NewReadOnlyTxn(c).Get(&result).
		Vars("getWithNames($name: string)", map[string]string{"$name": "wildan"}).
		Filter("allofterms(name, $name)").
		OrderAsc("name").
		OrderDesc("age")
	if err = query.Nodes(); err != nil {
		t.Error(err)
	}

	assert.Len(t, result, 10)

	for i, r := range result {
		if i < len(result)-1 {
			next := result[i+1]
			if r.Name == next.Name {
				if r.Age < next.Age {
					t.Error("wrong order")
				}
			}
		}
	}
}

func TestQueryBlock(t *testing.T) {
	c := newDgraphClient()
	if _, err := CreateSchema(c, &TestModel{}); err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	models := []*TestModel{}
	for i := 0; i < 10; i++ {
		models = append(models, &TestModel{
			Name:    fmt.Sprintf("wildan %d", i%10),
			Address: "Beverly Hills",
			Age:     i,
		})
	}

	tx := NewTxn(c).SetCommitNow()
	if _, err := tx.Mutate(&models); err != nil {
		t.Error(err)
		return
	}

	tx = NewReadOnlyTxn(c)

	type pagedResults struct {
		Paged    []*TestModel `json:"paged"`
		PageInfo []struct {
			Total int
		}
	}

	result := &pagedResults{}

	query := tx.
		Query(
			NewQuery().As("result").Var().Model(&TestModel{}).Filter(`anyofterms(name, $name)`),
			NewQuery().Name("paged").UID("result").First(2).Offset(2).All(1),
			NewQuery().Name("pageInfo").UID("result").Query(`{ total: count(uid) }`),
		).
		Vars("getByName($name: string)", map[string]string{"$name": "wildan"})

	if err := query.Scan(result); err != nil {
		t.Error(err)
	}

	assert.Len(t, result.Paged, 2)
	assert.Equal(t, result.PageInfo[0].Total, 10)
}

func TestGetNodesAndCount(t *testing.T) {
	c := newDgraphClient()
	if _, err := CreateSchema(c, &TestModel{}); err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	models := []*TestModel{}
	for i := 0; i < 5; i++ {
		models = append(models, &TestModel{
			Name:    fmt.Sprintf("wildan %d", i%10),
			Address: "Beverly Hills",
			Age:     i,
		}, &TestModel{
			Name:    fmt.Sprintf("alex %d", i%10),
			Address: "New York",
			Age:     i,
		})
	}

	tx := NewTxn(c).SetCommitNow()
	if _, err := tx.Mutate(&models); err != nil {
		t.Error(err)
		return
	}

	result := []*TestModel{}

	tx = NewReadOnlyTxn(c)
	count, err := tx.Get(&result).Filter(`anyofterms(name, "wildan")`).First(3).NodesAndCount()
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, result, 3)
	assert.Equal(t, 5, count)
}

func TestExpandAll(t *testing.T) {
	expectedDepthZero := `{
		uid
		dgraph.type
		expand(_all_)
	}`

	expectedDepthOne := `{
		uid
		dgraph.type
		expand(_all_) {
			uid
			dgraph.type
			expand(_all_)
		}
	}`

	expectedDepthTwo := `{
		uid
		dgraph.type
		expand(_all_) {
			uid
			dgraph.type
			expand(_all_) {
				uid
				dgraph.type
				expand(_all_)
			}
		}
	}`

	assert.Equal(t, expectedDepthZero, expandAll(0))
	assert.Equal(t, expectedDepthOne, expandAll(1))
	assert.Equal(t, expectedDepthTwo, expandAll(2))
}

func Test_parseQueryWithParams(t *testing.T) {
	type args struct {
		query  string
		params []interface{}
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "should parse the query with the params",
			args: args{
				query:  "{ valid: checkpwd(password, $1) }",
				params: []interface{}{"password)\nemail @filter(eq(email, email"},
			},
			want: `{ valid: checkpwd(password, "password)\nemail @filter(eq(email, email") }`,
		},
		{
			name: "should parse the query with multiple params",
			args: args{
				query:  "@filter(allofterms(name, $1) OR allofterms(lastname, $1) AND gt(age, $2))",
				params: []interface{}{"wildan", 3},
			},
			want: `@filter(allofterms(name, "wildan") OR allofterms(lastname, "wildan") AND gt(age, 3))`,
		},
		{
			name: "should parse uid as query param",
			args: args{
				query:  "@filter(uid($1) OR uid($2))",
				params: []interface{}{UID("0x1234"), UID("0xz)12}345")},
			},
			want: "@filter(uid(0x1234) OR uid(0x12345))",
		},
		{
			name: "should parse uids as query param",
			args: args{
				query:  "@filter(uid_in($1))",
				params: []interface{}{UIDs([]string{"0x1234", "0xz)12}345"})},
			},
			want: "@filter(uid_in(0x1234, 0x12345))",
		},
		{
			name: "should not parse the params, GraphQL named vars",
			args: args{
				query:  "{ valid: checkpwd(password, $name) }",
				params: []interface{}{"wildanjing"},
			},
			want: `{ valid: checkpwd(password, $name) }`,
		},
		{
			name: "should not panic on query slice out of bounds because invalid param string",
			args: args{
				query:  "{ valid: checkpwd(password, $",
				params: []interface{}{"password)\nemail @filter(eq(email, email"},
			},
			want: `{ valid: checkpwd(password, `,
		},
		{
			name: "should not panic on params slice out of bounds",
			args: args{
				query:  "{ valid: checkpwd(password, $1) }",
				params: []interface{}{},
			},
			want: `{ valid: checkpwd(password, ) }`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, parseQueryWithParams(tt.args.query, tt.args.params))
		})
	}
}
