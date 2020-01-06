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
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestNode struct {
	UID   string `json:"uid,omitempty"`
	Field string `json:"field,omitempty"`
}

type TestCustomNode struct {
	UID   string `json:"uid,omitempty"`
	Field string `json:"field,omitempty"`
}

type TestUnique struct {
	UID        string `json:"uid,omitempty"`
	Name       string `json:"name,omitempty" dgraph:"index=term"`
	Username   string `json:"username,omitempty" dgraph:"index=term unique"`
	Email      string `json:"email,omitempty" dgraph:"index=term unique notnull"`
	No         int    `json:"no,omitempty" dgraph:"index=int unique"`
	unexported int
}

func (n TestCustomNode) NodeType() string {
	return "CustomNodeType"
}

func TestAddNode(t *testing.T) {
	testData := &TestNode{"", "test"}

	c := newDgraphClient()
	defer dropAll(c)

	tx := NewTxn(c)

	err := tx.Mutate(testData, &MutateOptions{CommitNow: true})
	if err != nil {
		t.Error(err)
	}

	tx = NewTxn(c)

	query := `
	{
		data(func: type(TestNode)) {
			uid
			field
		}
	}
	`

	var result struct {
		Data []*TestNode
	}

	resp, err := tx.Txn().Query(context.Background(), query)
	if err != nil {
		t.Error(err)
	}

	if err := json.Unmarshal(resp.Json, &result); err != nil {
		t.Error(err)
	}

	assert.Len(t, result.Data, 1)
	assert.Equal(t, testData.UID, result.Data[0].UID)
}

func TestAddCustomNode(t *testing.T) {
	testData := TestCustomNode{"", "test"}

	c := newDgraphClient()
	defer dropAll(c)

	tx := NewTxn(c)
	err := tx.Mutate(&testData, &MutateOptions{CommitNow: true})
	if err != nil {
		t.Error(err)
	}

	tx = NewTxn(c)

	query := `
	query {
		data(func: type(CustomNodeType)) {
			uid
			field
		}
	}
	`

	var result struct {
		Data []TestCustomNode
	}

	resp, err := tx.Txn().Query(context.Background(), query)
	if err != nil {
		t.Error(err)
	}

	if err := json.Unmarshal(resp.Json, &result); err != nil {
		t.Error(err)
	}

	assert.Len(t, result.Data, 1)
	assert.Equal(t, testData.UID, result.Data[0].UID)
}

func TestAddNodeType(t *testing.T) {
	testData := TestNode{"", "test"}
	jsonData, err := marshalAndInjectType(&testData, false)
	if err != nil {
		t.Error(err)
	}

	// object
	expected := "{\"dgraph.type\":\"TestNode\",\"field\":\"test\"}"
	if string(jsonData) != expected {
		t.Errorf("expected %s got %s", expected, jsonData)
	}

	testDataArray := []TestNode{
		TestNode{"", "test"},
		TestNode{"", "test"},
	}

	// array
	expected = `[{"dgraph.type":"TestNode","field":"test"},{"dgraph.type":"TestNode","field":"test"}]`
	jsonData, err = marshalAndInjectType(&testDataArray, false)
	if err != nil {
		t.Error(err)
	}
	if string(jsonData) != expected {
		t.Errorf("expected %s got %s", expected, jsonData)
	}
}

func TestGetAllUniqueFields(t *testing.T) {
	testUnique := &TestUnique{
		Name:     "H3h3",
		Username: "wildan",
		Email:    "wildan2711@gmail.com",
		No:       4,
	}
	mType, err := newMutateType(testUnique)
	if err != nil {
		t.Error(err)
	}

	uniqueFields, err := mType.getAllUniqueFields(testUnique, false)
	if err != nil {
		t.Error(err)
	}
	assert.Len(t, uniqueFields, 3)
}

func TestCreateUnique(t *testing.T) {
	testUnique := []*TestUnique{
		&TestUnique{
			Name:     "H3h3",
			Username: "wildan",
			Email:    "wildan2711@gmail.com",
			No:       1,
		},
		&TestUnique{
			Name:     "PooDiePie",
			Username: "wildansyah",
			Email:    "wildansyah2711@gmail.com",
			No:       2,
		},
		&TestUnique{
			Name:     "Poopsie",
			Username: "wildani",
			Email:    "wildani@gmail.com",
			No:       3,
		},
	}

	c := newDgraphClient()
	schema, err := CreateSchema(c, &TestUnique{})
	if err != nil {
		t.Error(err)
	}
	defer dropAll(c)
	log.Println(schema)

	tx := NewTxn(c)
	err = tx.Create(&testUnique, &MutateOptions{CommitNow: true})
	require.NoError(t, err)
	assert.NotZero(t, testUnique[0].UID)
	assert.NotZero(t, testUnique[1].UID)
	assert.NotZero(t, testUnique[2].UID)

	testUnique2 := &TestUnique{
		Name:     "H3h3",
		Username: "wildan",
		Email:    "wildan2711@gmail.com",
		No:       1,
	}

	tx = NewTxn(c)
	err = tx.Create(testUnique2, &MutateOptions{CommitNow: true})
	require.NoError(t, err)

	assert.Zero(t, testUnique2.UID)

	testUnique3 := &TestUnique{
		Name:     "H343",
		Username: "wildanjing",
		Email:    "wildanjing2711@gmail.com",
		No:       99,
	}

	tx = NewTxn(c)
	err = tx.Create(testUnique3, &MutateOptions{CommitNow: true})
	require.NoError(t, err)
	assert.NotZero(t, testUnique3.UID)
}

func TestCreate(t *testing.T) {
	testUnique := []*TestUnique{
		&TestUnique{
			Name:     "H3h3",
			Username: "wildan",
			Email:    "wildan2711@gmail.com",
			No:       1,
		},
		&TestUnique{
			Name:     "PooDiePie",
			Username: "wildansyah",
			Email:    "wildansyah2711@gmail.com",
			No:       2,
		},
		&TestUnique{
			Name:     "Poopsie",
			Username: "wildani",
			Email:    "wildani@gmail.com",
			No:       3,
		},
	}

	c := newDgraphClient()
	if _, err := CreateSchema(c, &TestUnique{}); err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	tx := NewTxn(c)

	err := tx.Create(&testUnique)
	if err != nil {
		t.Error(err)
	}
	if err := tx.Commit(); err != nil {
		t.Error(err)
	}

	for _, el := range testUnique {
		if el.UID == "" {
			t.Error("uid is nil")
		}
	}

	testDuplicate := []*TestUnique{
		&TestUnique{
			Name:     "H3h3",
			Username: "wildanjing",
			Email:    "wildan2711@gmail.com",
			No:       4,
		},
		&TestUnique{
			Name:     "PooDiePie",
			Username: "wildansyah",
			Email:    "wildanodol2711@gmail.com",
			No:       5,
		},
		&TestUnique{
			Name:     "lalap",
			Username: "lalap",
			Email:    "lalap@gmail.com",
			No:       3,
		},
	}

	tx = NewTxn(c)

	var duplicates []UniqueError
	for _, data := range testDuplicate {
		err := tx.Create(data)
		if err != nil {
			if uniqueError, ok := err.(UniqueError); ok {
				duplicates = append(duplicates, uniqueError)
				continue
			}
			t.Error(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Error(err)
	}

	assert.Len(t, duplicates, 3)
	assert.Equal(t, duplicates[0].Field, "email")
	assert.Equal(t, duplicates[0].Value, "wildan2711@gmail.com")
	assert.Equal(t, duplicates[1].Field, "username")
	assert.Equal(t, duplicates[1].Value, "wildansyah")
	assert.Equal(t, duplicates[2].Field, "no")
	assert.Equal(t, duplicates[2].Value, 3)
}

func TestIsNull(t *testing.T) {
	assert.True(t, isNull(""))
	assert.True(t, isNull(0))
	assert.True(t, isNull(false))
	assert.True(t, isNull(nil))
}

func TestCreateNull(t *testing.T) {
	c := newDgraphClient()
	if _, err := CreateSchema(c, &TestUnique{}); err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	testUniqueNull := TestUnique{
		Name:     "H3h3",
		Username: "",
		Email:    "wildan2711@gmail.com",
		No:       4,
	}

	tx := NewTxn(c)
	if err := tx.Create(&testUniqueNull, &MutateOptions{CommitNow: true}); err != nil {
		t.Error(err)
	}

	testUniqueNullAgain := TestUnique{
		Name:     "tete",
		Username: "",
		Email:    "newemail@gmail.com",
		No:       5,
	}

	if err := tx.Create(&testUniqueNullAgain, &MutateOptions{CommitNow: true}); err != nil {
		t.Error(err)
	}
}

func TestUpdate(t *testing.T) {
	c := newDgraphClient()
	if _, err := CreateSchema(c, &TestUnique{}); err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	testUniques := []*TestUnique{
		&TestUnique{
			Name:     "haha",
			Username: "",
			Email:    "wildan2711@gmail.com",
			No:       1,
		},
		&TestUnique{
			Name:     "haha 2",
			Username: "wildancok2711",
			Email:    "wildancok2711@gmail.com",
			No:       2,
		},
	}

	tx := NewTxn(c)
	if err := tx.Create(&testUniques, &MutateOptions{CommitNow: true}); err != nil {
		t.Error(err)
	}

	testUpdate := testUniques[0]
	testUpdate.Username = "wildan2711"

	tx = NewTxn(c)
	if err := tx.Update(testUpdate, &MutateOptions{CommitNow: true}); err != nil {
		t.Error(err)
	}

	testUpdate2 := testUniques[1]
	testUpdate2.Username = "wildan2711"

	tx = NewTxn(c)
	if err := tx.Update(testUpdate2, &MutateOptions{CommitNow: true}); err != nil {
		if uniqueErr, ok := err.(UniqueError); ok {
			if uniqueErr.Field != "username" {
				t.Error("wrong unique field")
			}
		} else {
			t.Error(err)
		}
	} else {
		t.Error("must have unique error on username")
	}
}

type TestUniqueKeys struct {
	UID      string `json:"uid,omitempty"`
	Name     string `json:"name,omitempty" dgraph:"index=term"`
	Username string `json:"username,omitempty" dgraph:"index=term"`
	Email    string `json:"email,omitempty" dgraph:"index=term notnull"`
	No       int    `json:"no,omitempty" dgraph:"index=int"`
}

func (t TestUniqueKeys) UniqueKeys() []string {
	return []string{"username", "email", "no"}
}

func TestCreateCustomUniqueKeys(t *testing.T) {
	testUnique := []*TestUniqueKeys{
		&TestUniqueKeys{
			Name:     "H3h3",
			Username: "wildan",
			Email:    "wildan2711@gmail.com",
			No:       1,
		},
		&TestUniqueKeys{
			Name:     "PooDiePie",
			Username: "wildansyah",
			Email:    "wildansyah2711@gmail.com",
			No:       2,
		},
		&TestUniqueKeys{
			Name:     "Poopsie",
			Username: "wildani",
			Email:    "wildani@gmail.com",
			No:       3,
		},
	}

	c := newDgraphClient()
	if _, err := CreateSchema(c, &TestUnique{}); err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	tx := NewTxn(c)

	err := tx.Create(&testUnique)
	if err != nil {
		t.Error(err)
	}
	if err := tx.Commit(); err != nil {
		t.Error(err)
	}

	for _, el := range testUnique {
		if el.UID == "" {
			t.Error("uid is nil")
		}
	}

	testDuplicate := []*TestUniqueKeys{
		&TestUniqueKeys{
			Name:     "H3h3",
			Username: "wildanjing",
			Email:    "wildan2711@gmail.com",
			No:       4,
		},
		&TestUniqueKeys{
			Name:     "PooDiePie",
			Username: "wildansyah",
			Email:    "wildanodol2711@gmail.com",
			No:       5,
		},
		&TestUniqueKeys{
			Name:     "lalap",
			Username: "lalap",
			Email:    "lalap@gmail.com",
			No:       3,
		},
	}

	tx = NewTxn(c)

	var duplicates []UniqueError
	for _, data := range testDuplicate {
		err := tx.Create(data)
		if err != nil {
			if uniqueError, ok := err.(UniqueError); ok {
				duplicates = append(duplicates, uniqueError)
				continue
			}
			t.Error(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Error(err)
	}

	assert.Len(t, duplicates, 3)
	assert.Equal(t, duplicates[0].Field, "email")
	assert.Equal(t, duplicates[0].Value, "wildan2711@gmail.com")
	assert.Equal(t, duplicates[1].Field, "username")
	assert.Equal(t, duplicates[1].Value, "wildansyah")
	assert.Equal(t, duplicates[2].Field, "no")
	assert.Equal(t, duplicates[2].Value, 3)
}
