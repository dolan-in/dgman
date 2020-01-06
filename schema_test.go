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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type EnumType int

type GeoLoc struct {
	Type  string    `json:"type"`
	Coord []float64 `json:"coordinates"`
}

type User struct {
	UID        string       `json:"uid,omitempty"`
	Name       string       `json:"name,omitempty" dgraph:"index=term"`
	Username   string       `json:"username,omitempty" dgraph:"index=hash unique"`
	Email      string       `json:"email,omitempty" dgraph:"index=hash unique"`
	Password   string       `json:"password,omitempty"`
	Height     *int         `json:"height,omitempty"`
	Dob        *time.Time   `json:"dob,omitempty"`
	Status     EnumType     `json:"status,omitempty" dgraph:"type=int"`
	Created    time.Time    `json:"created,omitempty"`
	Dates      []time.Time  `json:"dates,omitempty"`
	DatesPtr   []*time.Time `json:"dates_ptr,omitempty"`
	Mobiles    []string     `json:"mobiles,omitempty"`
	Schools    []School     `json:"schools,omitempty" dgraph:"count"`
	SchoolsPtr []*School    `json:"schools_ptr,omitempty" dgraph:"count reverse"`
	School     School       `json:"school" dgraph:"count reverse"`
	SchoolPtr  *School      `json:"school_ptr" dgraph:"count reverse"`
	*Anonymous
}

type Anonymous struct {
	Field1 string `json:"field_1,omitempty"`
	Field2 string `json:"field_2,omitempty"`
}

type School struct {
	UID      string  `json:"uid,omitempty"`
	Name     string  `json:"name,omitempty"`
	Location *GeoLoc `json:"location,omitempty" dgraph:"type=geo"` // test passing type
}

type OneToOne struct {
	UID    string `json:"uid,omitempty"`
	School School `json:"school,omitempty"`
}

type NewUser struct {
	UID      string `json:"uid,omitempty"`
	Username string `json:"username,omitempty" dgraph:"index=term"`
	Email    string `json:"email,omitempty" dgraph:"index=term"`
	Password string `json:"password,omitempty"`
}

func TestMarshalSchema(t *testing.T) {
	typeSchema := NewTypeSchema()
	typeSchema.Marshal(&User{})
	types, schema := typeSchema.Types, typeSchema.Schema
	assert.Len(t, schema, 16)
	assert.Contains(t, schema, "username")
	assert.Contains(t, schema, "email")
	assert.Contains(t, schema, "password")
	assert.Contains(t, schema, "name")
	assert.Contains(t, schema, "height")
	assert.Contains(t, schema, "mobiles")
	assert.Contains(t, schema, "status")
	assert.Contains(t, schema, "dob")
	assert.Contains(t, schema, "created")
	assert.Contains(t, schema, "dates")
	assert.Contains(t, schema, "dates_ptr")
	assert.Contains(t, schema, "location")
	assert.Contains(t, schema, "schools")
	assert.Contains(t, schema, "schools_ptr")
	assert.Contains(t, schema, "school")
	assert.Contains(t, schema, "school_ptr")
	assert.Equal(t, "username: string @index(hash) @upsert .", schema["username"].String())
	assert.Equal(t, "email: string @index(hash) @upsert .", schema["email"].String())
	assert.Equal(t, "password: string .", schema["password"].String())
	assert.Equal(t, "name: string @index(term) .", schema["name"].String())
	assert.Equal(t, "mobiles: [string] .", schema["mobiles"].String())
	assert.Equal(t, "schools: [uid] @count .", schema["schools"].String())
	assert.Equal(t, "schools_ptr: [uid] @count @reverse .", schema["schools_ptr"].String())
	assert.Equal(t, "school: uid @count @reverse .", schema["school"].String())
	assert.Equal(t, "school_ptr: uid @count @reverse .", schema["school_ptr"].String())
	assert.Equal(t, "status: int .", schema["status"].String())
	assert.Equal(t, "height: int .", schema["height"].String())
	assert.Equal(t, "dob: datetime .", schema["dob"].String())
	assert.Equal(t, "created: datetime .", schema["created"].String())
	assert.Equal(t, "dates: [datetime] .", schema["dates"].String())
	assert.Equal(t, "dates_ptr: [datetime] .", schema["dates_ptr"].String())
	assert.Equal(t, "location: geo .", schema["location"].String())

	assert.Len(t, types, 2)
	assert.Contains(t, types, "User")
	assert.Contains(t, types, "School")
	assert.Len(t, types["User"], 15)
	assert.Len(t, types["School"], 2)
}

func TestGetNodeType(t *testing.T) {
	nodeTypeStruct := GetNodeType(TestNode{})
	nodeTypePtr := GetNodeType(&TestNode{})
	nodeTypeSlice := GetNodeType([]TestNode{})
	nodeTypeSlicePtr := GetNodeType([]*TestNode{})

	assert.Equal(t, "TestNode", nodeTypeStruct)
	assert.Equal(t, "TestNode", nodeTypePtr)
	assert.Equal(t, "TestNode", nodeTypeSlice)
	assert.Equal(t, "TestNode", nodeTypeSlicePtr)
}

func TestCreateSchema(t *testing.T) {
	c := newDgraphClient()
	defer dropAll(c)

	firstSchema, err := CreateSchema(c, &User{})
	if err != nil {
		t.Error(err)
	}
	assert.Len(t, firstSchema.Schema, 16)
	assert.Len(t, firstSchema.Types, 2)

	secondSchema, err := CreateSchema(c, &NewUser{})
	if err != nil {
		t.Error(err)
	}
	// conflicts should be ignored
	assert.Len(t, secondSchema.Schema, 0)
	assert.Len(t, secondSchema.Types, 1)

	firstSchema, err = CreateSchema(c, &User{})
	if err != nil {
		t.Error(err)
	}
	assert.Len(t, firstSchema.Schema, 0)
	assert.Len(t, firstSchema.Types, 0)
}

func TestMutateSchema(t *testing.T) {
	c := newDgraphClient()
	defer dropAll(c)

	firstSchema, err := CreateSchema(c, &User{})
	if err != nil {
		t.Error(err)
	}
	assert.Len(t, firstSchema.Schema, 16)
	assert.Len(t, firstSchema.Types, 2)

	secondSchema, err := MutateSchema(c, &NewUser{})
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, secondSchema.Schema, 3)
	assert.Len(t, secondSchema.Types, 1)

	updatedSchema, err := fetchExistingSchema(c)
	if err != nil {
		t.Error(err)
	}

	for _, schema := range updatedSchema {
		m := secondSchema.Schema
		if s, ok := m[schema.Predicate]; ok {
			assert.Equal(t, s, schema)
		}
	}
}

func TestOneToOneSchema(t *testing.T) {
	c := newDgraphClient()
	defer dropAll(c)

	schema, err := CreateSchema(c, &OneToOne{})
	if err != nil {
		t.Error(err)
	}
	assert.Len(t, schema.Schema, 3)
}
