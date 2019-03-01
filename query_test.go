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
	"fmt"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestModel struct {
	UID     string     `json:"uid"`
	Name    string     `json:"name" dgraph:"index=term"`
	Address string     `json:"address"`
	Age     int        `json:"age" dgraph:"index=int"`
	Dead    bool       `json:"dead"`
	Edges   []TestEdge `json:"edges"`
}

type TestEdge struct {
	UID   string `json:"uid"`
	Level string `json"level"`
}

func TestGetByUID(t *testing.T) {
	source := &TestModel{
		Name:    "wildanjing",
		Address: "Beverly Hills",
		Age:     17,
	}

	c := newDgraphClient()
	defer dropAll(c)

	tx := c.NewTxn()

	ctx := context.Background()

	err := Mutate(ctx, tx, source, MutateOptions{CommitNow: true})
	if err != nil {
		t.Error(err)
	}

	dst := &TestModel{}
	tx = c.NewTxn()
	if err := Get(ctx, tx, dst).UID(source.UID).Node(); err != nil {
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

	tx := c.NewTxn()

	ctx := context.Background()

	err := Mutate(ctx, tx, source, MutateOptions{CommitNow: true})
	if err != nil {
		t.Error(err)
	}

	dst := &TestModel{}
	tx = c.NewTxn()
	if err := Get(ctx, tx, dst).Filter(`allofterms(name, "wildan")`).Node(); err != nil {
		t.Error(err)
	}

	assert.Equal(t, source.Name, dst.Name)
	assert.Equal(t, source.Address, dst.Address)
	assert.Equal(t, source.Age, dst.Age)
	assert.Equal(t, source.Dead, dst.Dead)

	dst = &TestModel{}
	tx = c.NewTxn()
	if err := Get(ctx, tx, dst).Filter(`allofterms(name, "onono")`).Node(); err != ErrNodeNotFound {
		t.Error(err)
	}
}

func TestFind(t *testing.T) {
	source := []TestModel{
		TestModel{
			Name:    "wildan anjing",
			Address: "Beverly Hills",
			Age:     17,
		},
		TestModel{
			Name:    "moh wildan",
			Address: "Beverly Hills",
			Age:     17,
		},
		TestModel{
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

	tx := c.NewTxn()

	ctx := context.Background()

	err := Mutate(ctx, tx, &source)
	if err != nil {
		t.Error(err)
	}
	tx.Commit(ctx)

	var dst []TestModel
	tx = c.NewTxn()
	if err := Get(ctx, tx, &dst).Filter(`allofterms(name, "wildan")`).Nodes(); err != nil {
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

	tx := c.NewTxn()

	ctx := context.Background()

	err := Create(ctx, tx, &source)
	if err != nil {
		t.Error(err)
	}
	tx.Commit(ctx)

	source2 := TestEdge{
		Level: "one",
	}

	tx = c.NewTxn()
	err = Create(ctx, tx, &source2)
	if err != nil {
		t.Error(err)
	}
	tx.Commit(ctx)

	source.Edges = []TestEdge{source2}

	tx = c.NewTxn()
	err = Mutate(ctx, tx, &source)
	if err != nil {
		t.Error(err)
	}
	tx.Commit(ctx)

	var dst TestModel
	tx = c.NewTxn()
	q := Get(ctx, tx, &dst).Query(`@filter(allofterms(name, "wildan")) {
		uid
		expand(_all_) {
			uid
			expand(_all_)
		}
	}`)
	log.Println(q)
	if err := q.Node(); err != nil {
		t.Error(err)
	}

	assert.Equal(t, dst.UID, source.UID)
	assert.Len(t, dst.Edges, 1)
	assert.Equal(t, dst.Edges[0].UID, source2.UID)
	assert.Equal(t, dst.Edges[0].Level, source2.Level)
}

func TestPagination(t *testing.T) {
	models := []TestModel{}
	for i := 0; i < 20; i++ {
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

	tx := c.NewTxn()

	ctx := context.Background()
	err := Mutate(ctx, tx, &models)
	if err != nil {
		t.Error(err)
	}
	tx.Commit(ctx)

	result := []TestModel{}
	query := Get(ctx, c.NewTxn(), &result).
		Vars("getWithNames($name: string)", map[string]string{"$name": "wildan"}).
		Filter("allofterms(name, $name)").
		First(10)
	log.Println(query)
	if err = query.Nodes(); err != nil {
		t.Error(err)
	}

	assert.Len(t, result, 10)

	result = []TestModel{}
	query = Get(ctx, c.NewTxn(), &result).
		Vars("getWithNames($name: string)", map[string]string{"$name": "nonon"}).
		Filter("allofterms(name, $name)").
		First(10)
	if err = query.Nodes(); err != nil {
		t.Error(err)
	}

	assert.Len(t, result, 0)

	result = []TestModel{}
	query = Get(ctx, c.NewTxn(), &result).
		Vars("getWithNames($name: string)", map[string]string{"$name": "wildan"}).
		Filter("allofterms(name, $name)").
		First(255555)
	if err = query.Nodes(); err != nil {
		t.Error(err)
	}

	assert.Len(t, result, 20)
}

func TestOrder(t *testing.T) {
	models := []*TestModel{}
	for i := 0; i < 20; i++ {
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

	tx := c.NewTxn()

	ctx := context.Background()
	err := Create(ctx, tx, &models)
	if err != nil {
		t.Error(err)
	}
	tx.Commit(ctx)

	result := []*TestModel{}
	query := Get(ctx, c.NewTxn(), &result).
		Vars("getWithNames($name: string)", map[string]string{"$name": "wildan"}).
		Filter("allofterms(name, $name)").
		OrderAsc("age")
	log.Println(query)
	if err = query.Nodes(); err != nil {
		t.Error(err)
	}

	assert.Len(t, result, 20)

	for i, r := range result {
		log.Println(*r)
		assert.Equal(t, models[i], r)
	}

	result = []*TestModel{}
	query = Get(ctx, c.NewTxn(), &result).
		Vars("getWithNames($name: string)", map[string]string{"$name": "wildan"}).
		Filter("allofterms(name, $name)").
		OrderAsc("name").
		OrderDesc("age")
	log.Println(query)
	if err = query.Nodes(); err != nil {
		t.Error(err)
	}

	assert.Len(t, result, 20)

	for i, r := range result {
		log.Println(*r)
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
