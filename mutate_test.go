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
	"testing"

	"github.com/stretchr/testify/assert"
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
	Name       string `json:"name,omitempty"`
	Username   string `json:"username,omitempty" dgraph:"index=term unique"`
	Email      string `json:"email,omitempty" dgraph:"index=term unique notnull"`
	No         int    `json:"no,omitempty" dgraph:"index=int unique"`
	unexported int
}

func (n TestCustomNode) NodeType() string {
	return "custom_node_type"
}

func TestAddNode(t *testing.T) {
	testData := TestNode{"", "test"}

	c := newDgraphClient()
	defer dropAll(c)

	tx := c.NewTxn()

	err := Mutate(context.Background(), tx, &testData, MutateOptions{CommitNow: true})
	if err != nil {
		t.Error(err)
	}

	tx = c.NewTxn()

	query := `
	{
		data(func: has(test_node)) {
			uid
			field
		}
	}
	`

	var result struct {
		Data []TestNode
	}

	resp, err := tx.Query(context.Background(), query)
	if err != nil {
		t.Error(err)
	}

	if err := json.Unmarshal(resp.Json, &result); err != nil {
		t.Error(err)
	}

	assert.Len(t, result.Data, 1)
	assert.Equal(t, testData.UID, result.Data[0].UID)
}

func TestAddCustomeNode(t *testing.T) {
	testData := TestCustomNode{"", "test"}

	c := newDgraphClient()
	defer dropAll(c)

	tx := c.NewTxn()
	err := Mutate(context.Background(), tx, &testData, MutateOptions{CommitNow: true})
	if err != nil {
		t.Error(err)
	}

	tx = c.NewTxn()

	query := `
	query {
		data(func: has(custom_node_type)) {
			uid
			field
		}
	}
	`

	var result struct {
		Data []TestCustomNode
	}

	resp, err := tx.Query(context.Background(), query)
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
	expected := "{\"test_node\":\"\",\"field\":\"test\"}"
	if string(jsonData) != expected {
		t.Errorf("expected %s got %s", expected, jsonData)
	}

	testDataArray := []TestNode{
		TestNode{"", "test"},
		TestNode{"", "test"},
	}

	// array
	expected = `[{"test_node":"","field":"test"},{"test_node":"","field":"test"}]`
	jsonData, err = marshalAndInjectType(&testDataArray, false)
	if err != nil {
		t.Error(err)
	}
	if string(jsonData) != expected {
		t.Errorf("expected %s got %s", expected, jsonData)
	}
}

func TestGetNodeType(t *testing.T) {
	nodeTypeStruct := GetNodeType(TestNode{})
	nodeTypePtr := GetNodeType(&TestNode{})
	nodeTypeSlice := GetNodeType([]TestNode{})
	nodeTypeSlicePtr := GetNodeType([]*TestNode{})

	assert.Equal(t, nodeTypeStruct, "test_node")
	assert.Equal(t, nodeTypePtr, "test_node")
	assert.Equal(t, nodeTypeSlice, "test_node")
	assert.Equal(t, nodeTypeSlicePtr, "test_node")
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

	uniqueFields, err := mType.getAllUniqueFields(testUnique)
	if err != nil {
		t.Error(err)
	}
	assert.Len(t, uniqueFields, 3)
}

func TestCreate(t *testing.T) {
	testUnique := []TestUnique{
		TestUnique{
			Name:     "H3h3",
			Username: "wildan",
			Email:    "wildan2711@gmail.com",
			No:       1,
		},
		TestUnique{
			Name:     "PooDiePie",
			Username: "wildansyah",
			Email:    "wildansyah2711@gmail.com",
			No:       2,
		},
		TestUnique{
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

	tx := c.NewTxn()

	err := Create(context.Background(), tx, &testUnique)
	if err != nil {
		t.Error(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Error(err)
	}

	for _, el := range testUnique {
		if el.UID == "" {
			t.Error("uid is nil")
		}
	}

	testDuplicate := []TestUnique{
		TestUnique{
			Name:     "H3h3",
			Username: "wildanjing",
			Email:    "wildan2711@gmail.com",
			No:       4,
		},
		TestUnique{
			Name:     "PooDiePie",
			Username: "wildansyah",
			Email:    "wildanodol2711@gmail.com",
			No:       5,
		},
		TestUnique{
			Name:     "lalap",
			Username: "lalap",
			Email:    "lalap@gmail.com",
			No:       3,
		},
	}

	tx = c.NewTxn()

	var duplicates []UniqueError
	for _, data := range testDuplicate {
		err := Create(context.Background(), tx, &data)
		if err != nil {
			if uniqueError, ok := err.(UniqueError); ok {
				duplicates = append(duplicates, uniqueError)
				continue
			}
			t.Error(err)
		}
	}
	if err := tx.Commit(context.Background()); err != nil {
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

	if err := Create(context.Background(), c.NewTxn(), &testUniqueNull, MutateOptions{CommitNow: true}); err != nil {
		t.Error(err)
	}

	testUniqueNullAgain := TestUnique{
		Name:     "tete",
		Username: "",
		Email:    "newemail@gmail.com",
		No:       5,
	}

	if err := Create(context.Background(), c.NewTxn(), &testUniqueNullAgain, MutateOptions{CommitNow: true}); err != nil {
		t.Error(err)
	}
}

func TestCreateNotNull(t *testing.T) {
	c := newDgraphClient()
	if _, err := CreateSchema(c, &TestUnique{}); err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	testNotNull := TestUnique{
		Name:     "H3h3",
		Username: "wildan2711",
		Email:    "",
		No:       4,
	}

	if err := Create(context.Background(), c.NewTxn(), &testNotNull, MutateOptions{CommitNow: true}); err != nil {
		if nullErr, ok := err.(NotNullError); ok {
			if nullErr.Field == "email" {
				return
			}
		}
		t.Error(err)
	}
}
