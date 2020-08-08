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
	"time"

	"github.com/stretchr/testify/assert"
)

type TestNode struct {
	UID   string   `json:"uid,omitempty"`
	Field string   `json:"field,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

type TestCustomNode struct {
	UID   string   `json:"uid,omitempty"`
	Field string   `json:"field,omitempty"`
	DType []string `json:"dgraph.type,omitempty" dgraph:"CustomNodeType"`
}

type TestUnique struct {
	UID        string `json:"uid,omitempty"`
	Name       string `json:"name,omitempty" dgraph:"index=term"`
	Username   string `json:"username,omitempty" dgraph:"index=term unique"`
	Email      string `json:"email,omitempty" dgraph:"index=term unique notnull"`
	No         int    `json:"no,omitempty" dgraph:"index=int unique"`
	unexported int
	DType      []string `json:"dgraph.type,omitempty"`
}

type TestDTypeString struct {
	UID   string `json:"uid,omitempty"`
	Name  string `json:"name,omitempty"`
	DType string `json:"dgraph.type,omitempty"`
}

type TestDTypeSlice struct {
	UID          string                 `json:"uid,omitempty"`
	Name         string                 `json:"name,omitempty"`
	Edge         TestDTypeSliceInner    `json:"edge,omitempty"`
	PtrEdge      *TestDTypeSliceInner   `json:"ptr_edge,omitempty"`
	SliceEdge    []TestDTypeSliceInner  `json:"slice_edge,omitempty"`
	SlicePtrEdge []*TestDTypeSliceInner `json:"slice_ptr_edge,omitempty"`
	Time         *time.Time             `json:"time,omitempty"`
	Times        []*time.Time           `json:"times,omitempty"`
	DType        []string               `json:"dgraph.type,omitempty"`
}

type TestDTypeAnonymous struct {
	Field string `json:"field,omitempty"`
	TestDTypeSliceInner
	DType []string `json:"dgraph.type,omitempty"`
}

type TestDTypeSliceInner struct {
	UID   string   `json:"uid,omitempty"`
	Name  string   `json:"name,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

func Test_marshalAndInjectType(t *testing.T) {
	type args struct {
		data interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name:    "should inject string in dgraph.type for struct",
			args:    args{&TestDTypeString{Name: "wildan"}},
			want:    []byte(`{"name":"wildan","dgraph.type":"TestDTypeString"}`),
			wantErr: false,
		},
		{
			name: "should inject slice of string in dgraph.type for struct",
			args: args{&TestDTypeSlice{
				Name:    "wildan",
				Edge:    TestDTypeSliceInner{Name: "wildanjing"},
				PtrEdge: &TestDTypeSliceInner{Name: "wildanjing2"},
				SliceEdge: []TestDTypeSliceInner{
					{Name: "wildanjing3"},
				},
				SlicePtrEdge: []*TestDTypeSliceInner{
					{Name: "wildanjing4"},
				},
			}},
			want:    []byte(`{"name":"wildan","edge":{"name":"wildanjing","dgraph.type":["TestDTypeSliceInner"]},"ptr_edge":{"name":"wildanjing2","dgraph.type":["TestDTypeSliceInner"]},"slice_edge":[{"name":"wildanjing3","dgraph.type":["TestDTypeSliceInner"]}],"slice_ptr_edge":[{"name":"wildanjing4","dgraph.type":["TestDTypeSliceInner"]}],"dgraph.type":["TestDTypeSlice"]}`),
			wantErr: false,
		},
		{
			name: "should inject slice of string in dgraph.type for slice",
			args: args{&[]TestDTypeSlice{
				{
					Name:    "wildan",
					Edge:    TestDTypeSliceInner{Name: "wildanjing"},
					PtrEdge: &TestDTypeSliceInner{Name: "wildanjing2"},
				},
				{
					Name:    "wildan",
					Edge:    TestDTypeSliceInner{Name: "wildanjing"},
					PtrEdge: &TestDTypeSliceInner{Name: "wildanjing2"},
				},
			}},
			want:    []byte(`[{"name":"wildan","edge":{"name":"wildanjing","dgraph.type":["TestDTypeSliceInner"]},"ptr_edge":{"name":"wildanjing2","dgraph.type":["TestDTypeSliceInner"]},"dgraph.type":["TestDTypeSlice"]},{"name":"wildan","edge":{"name":"wildanjing","dgraph.type":["TestDTypeSliceInner"]},"ptr_edge":{"name":"wildanjing2","dgraph.type":["TestDTypeSliceInner"]},"dgraph.type":["TestDTypeSlice"]}]`),
			wantErr: false,
		},
		{
			name: "should parse anonymous structs with dgraph.type",
			args: args{&TestDTypeAnonymous{
				TestDTypeSliceInner: TestDTypeSliceInner{
					Name: "wildan",
				},
			}},
			want: []byte(`{"name":"wildan","dgraph.type":["TestDTypeAnonymous"]}`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := marshalAndInjectType(tt.args.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("marshalAndInjectTypeV2() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Equal(t, string(tt.want), string(got))
		})
	}
}

func TestUniqueError_Error(t *testing.T) {
	assert.EqualError(t, &UniqueError{NodeType: "User", Field: "username", Value: "wildanjing", UID: "0x1234"}, "User with username=wildanjing already exists at uid=0x1234")
}

func TestAddNode(t *testing.T) {
	testData := &TestNode{Field: "test"}

	c := newDgraphClient()
	defer dropAll(c)

	tx := NewTxn(c)

	err := tx.Mutate(testData, true)
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
	testData := TestCustomNode{Field: "test"}

	c := newDgraphClient()
	defer dropAll(c)

	tx := NewTxn(c)
	err := tx.Mutate(&testData, true)
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

func TestCreate(t *testing.T) {
	testUnique := []TestUnique{
		{
			Name:     "H3h3",
			Username: "wildan",
			Email:    "wildan2711@gmail.com",
			No:       1,
		},
		{
			Name:     "PooDiePie",
			Username: "wildansyah",
			Email:    "wildansyah2711@gmail.com",
			No:       2,
		},
		{
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

	testDuplicate := []TestUnique{
		{
			Name:     "H3h3",
			Username: "wildanjing",
			Email:    "wildan2711@gmail.com",
			No:       4,
		},
		{
			Name:     "PooDiePie",
			Username: "wildansyah",
			Email:    "wildanodol2711@gmail.com",
			No:       5,
		},
		{
			Name:     "lalap",
			Username: "lalap",
			Email:    "lalap@gmail.com",
			No:       3,
		},
	}

	tx = NewTxn(c)

	var duplicates []*UniqueError
	for _, data := range testDuplicate {
		err := tx.Create(&data)
		if err != nil {
			if uniqueError, ok := err.(*UniqueError); ok {
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

	if err := NewTxn(c).Create(&testUniqueNull, true); err != nil {
		t.Error(err)
	}

	testUniqueNullAgain := TestUnique{
		Name:     "tete",
		Username: "",
		Email:    "newemail@gmail.com",
		No:       5,
	}

	if err := NewTxn(c).Create(&testUniqueNullAgain, true); err != nil {
		t.Error(err)
	}
}

func TestUpdate(t *testing.T) {
	c := newDgraphClient()
	if _, err := CreateSchema(c, &TestUnique{}); err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	testUniques := []TestUnique{
		{
			Name:     "haha",
			Username: "",
			Email:    "wildan2711@gmail.com",
			No:       1,
		},
		{
			Name:     "haha 2",
			Username: "wildancok2711",
			Email:    "wildancok2711@gmail.com",
			No:       2,
		},
	}

	tx := NewTxn(c)
	if err := tx.Create(&testUniques, true); err != nil {
		t.Error(err)
	}

	testUpdate := testUniques[0]
	testUpdate.Username = "wildan2711"

	tx = NewTxn(c)
	if err := tx.Update(&testUpdate, true); err != nil {
		t.Error(err)
	}

	testUpdate2 := testUniques[1]
	testUpdate2.Username = "wildan2711"

	tx = NewTxn(c)
	if err := tx.Update(&testUpdate2, true); err != nil {
		if uniqueErr, ok := err.(*UniqueError); ok {
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

func TestUpsert(t *testing.T) {
	c := newDgraphClient()
	if _, err := CreateSchema(c, &TestUnique{}); err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	testUpsert := &TestUnique{
		Name:     "haha",
		Username: "wildan2711",
		Email:    "wildan2711@gmail.com",
		No:       1,
	}

	tx := NewTxn(c)
	if err := tx.Upsert(testUpsert, "username", true); err != nil {
		t.Error(err)
	}

	assert.NotZero(t, testUpsert.UID)

	testUpsert2 := &TestUnique{
		Name:     "wildanjing",
		Username: "wildan2711",
		Email:    "wildancok2711@gmail.com",
		No:       2,
	}

	tx = NewTxn(c)
	if err := tx.Upsert(testUpsert2, "username", true); err != nil {
		t.Error(err)
	}

	assert.Equal(t, testUpsert.UID, testUpsert2.UID)

	// check if the upsert succeeded
	result := &TestUnique{}
	if err := NewReadOnlyTxn(c).Get(result).UID(testUpsert.UID).Node(); err != nil {
		t.Error(err)
	}

	assert.Equal(t, testUpsert2, result)

	// make sure unique checking still holds
	testCreate := &TestUnique{
		Name:     "wildanjing",
		Username: "wildancok2711",
		Email:    "wildan2711@gmail.com",
		No:       3,
	}

	if err := NewTxn(c).Create(testCreate, true); err != nil {
		t.Error(err)
	}

	testUpsert3 := &TestUnique{
		Name:     "wildanjing",
		Username: "wildancok2711",
		Email:    "wildancok2711@gmail.com",
		No:       4,
	}

	tx = NewTxn(c)
	if err := tx.Upsert(testUpsert3, "username", true); err != nil {
		if uniqueErr, ok := err.(*UniqueError); ok {
			if uniqueErr.Field != "email" {
				t.Error("wrong unique field")
			}
		} else {
			t.Error(err)
		}
	} else {
		t.Error("must have unique error on email")
	}
}

func TestCreateOrGet(t *testing.T) {
	c := newDgraphClient()
	if _, err := CreateSchema(c, &TestUnique{}); err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	testUniques := []TestUnique{
		{
			Name:     "haha",
			Username: "wilcok",
			Email:    "wildan2711@gmail.com",
			No:       1,
		},
		{
			Name:     "haha 2",
			Username: "wildancok2711",
			Email:    "wildancok2711@gmail.com",
			No:       2,
		},
	}

	tx := NewTxn(c)
	if err := tx.Create(&testUniques, true); err != nil {
		t.Error(err)
	}

	testCreateOrGet := testUniques[1]
	testCreateOrGet.Email = "wildan2711@gmail.com"

	tx = NewTxn(c)
	if err := tx.CreateOrGet(&testCreateOrGet, "email", true); err != nil {
		t.Error(err)
	}

	assert.Equal(t, testUniques[0], testCreateOrGet)
}
