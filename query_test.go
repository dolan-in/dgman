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
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestModel struct {
	UID     string `json:"uid"`
	Name    string `json:"name" dgraph:"index=term"`
	Address string `json:"address"`
	Age     int    `json:"age"`
	Dead    bool   `json:"dead"`
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
	if err := GetByUID(ctx, tx, source.UID, dst); err != nil {
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
	if err := GetByFilter(ctx, tx, `allofterms(name, "wildan")`, dst); err != nil {
		t.Error(err)
	}

	assert.Equal(t, source.Name, dst.Name)
	assert.Equal(t, source.Address, dst.Address)
	assert.Equal(t, source.Age, dst.Age)
	assert.Equal(t, source.Dead, dst.Dead)
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
	if err := Find(ctx, tx, `allofterms(name, "wildan")`, &dst); err != nil {
		t.Error(err)
	}

	assert.Len(t, dst, 2)
}
