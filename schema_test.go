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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type EnumType int

type CustomTime time.Time

func (c CustomTime) SchemaType() string {
	return "datetime"
}

type GeoLoc struct {
	Type  string    `json:"type"`
	Coord []float64 `json:"coordinates"`
}

type User struct {
	UID        string       `json:"uid,omitempty"`
	Name       string       `json:"name,omitempty" dgraph:"index=term"`
	Username   string       `json:"username,omitempty" dgraph:"index=hash unique"`
	Email      string       `json:"email,omitempty" dgraph:"index=hash unique"`
	Noconflict string       `json:"noconflict,omitempty" dgraph:"index=hash noconflict"`
	Password   string       `json:"password,omitempty"`
	Review     string       `json:"review" dgraph:"index=fulltext lang"`
	ReviewEn   string       `json:"review@en"` // should not be parsed
	ReviewDe   string       `json:"review@de"` // should not be parsed
	Height     *int         `json:"height,omitempty"`
	IsAdmin    bool         `json:"is_admin,omitempty"`
	CustomTime CustomTime   `json:"custom_time,omitempty"`
	Dob        *time.Time   `json:"dob,omitempty"`
	Status     EnumType     `json:"status,omitempty" dgraph:"type=int"`
	Created    *time.Time   `json:"created,omitempty"`
	Dates      []time.Time  `json:"dates,omitempty"`
	DatesPtr   []*time.Time `json:"dates_ptr,omitempty"`
	Mobiles    []string     `json:"mobiles,omitempty"`
	Schools    []School     `json:"schools,omitempty" dgraph:"count"`
	SchoolsPtr []*School    `json:"schools_ptr,omitempty" dgraph:"count reverse"`
	School     School       `json:"school" dgraph:"count reverse"`
	SchoolPtr  *School      `json:"school_ptr" dgraph:"count reverse"`
	Friends    []User       `json:"friends"`             // test recursive, will panic if fail
	Object     interface{}  `json:"object" dgraph:"uid"` // test interface, will panic if fail
	*Anonymous
	DType []string `json:"dgraph.type"`
}

type Anonymous struct {
	Field1 string `json:"field_1,omitempty"`
	Field2 string `json:"field_2,omitempty"`
}

type School struct {
	UID      string   `json:"uid,omitempty"`
	Name     string   `json:"name,omitempty" dgraph:"index=term"`
	Location *GeoLoc  `json:"location,omitempty" dgraph:"type=geo"` // test passing type
	DType    []string `json:"dgraph.type"`
}

type OneToOne struct {
	UID    string   `json:"uid,omitempty"`
	School School   `json:"school,omitempty"`
	DType  []string `json:"dgraph.type"`
}

type NewUser struct {
	UID      string   `json:"uid,omitempty"`
	Username string   `json:"username,omitempty" dgraph:"index=term"`
	Email    string   `json:"email,omitempty" dgraph:"index=term"`
	Password string   `json:"password,omitempty"`
	DType    []string `json:"dgraph.type"`
}

func TestMarshalSchema(t *testing.T) {
	typeSchema := NewTypeSchema()
	typeSchema.Marshal("", &User{})
	types, schema := typeSchema.Types, typeSchema.Schema
	assert.Equal(t, "username: string @index(hash) @upsert .", schema["username"].String())
	assert.Equal(t, "email: string @index(hash) @upsert .", schema["email"].String())
	assert.Equal(t, "noconflict: string @index(hash) @noconflict .", schema["noconflict"].String())
	assert.Equal(t, "password: string .", schema["password"].String())
	assert.Equal(t, "name: string @index(term) .", schema["name"].String())
	assert.Equal(t, "mobiles: [string] .", schema["mobiles"].String())
	assert.Equal(t, "schools: [uid] @count .", schema["schools"].String())
	assert.Equal(t, "schools_ptr: [uid] @count @reverse .", schema["schools_ptr"].String())
	assert.Equal(t, "school: uid @count @reverse .", schema["school"].String())
	assert.Equal(t, "school_ptr: uid @count @reverse .", schema["school_ptr"].String())
	assert.Equal(t, "status: int .", schema["status"].String())
	assert.Equal(t, "review: string @index(fulltext) @lang .", schema["review"].String())
	assert.Equal(t, "height: int .", schema["height"].String())
	assert.Equal(t, "custom_time: datetime .", schema["custom_time"].String())
	assert.Equal(t, "dob: datetime .", schema["dob"].String())
	assert.Equal(t, "is_admin: bool .", schema["is_admin"].String())
	assert.Equal(t, "created: datetime .", schema["created"].String())
	assert.Equal(t, "dates: [datetime] .", schema["dates"].String())
	assert.Equal(t, "dates_ptr: [datetime] .", schema["dates_ptr"].String())
	assert.Equal(t, "location: geo .", schema["location"].String())
	assert.Equal(t, "field_1: string .", schema["field_1"].String())
	assert.Equal(t, "field_2: string .", schema["field_2"].String())
	assert.Equal(t, "friends: [uid] .", schema["friends"].String())
	assert.Equal(t, "object: uid .", schema["object"].String())

	assert.NotContains(t, schema, "review@en")
	assert.NotContains(t, schema, "review@en")

	assert.Contains(t, types, "User")
	assert.Contains(t, types, "School")

	// anonymous fields should be included in type
	assert.Contains(t, types["User"], "field_1")
	assert.Contains(t, types["User"], "field_2")
}

func TestGetNodeType(t *testing.T) {
	nodeTypeStruct := GetNodeType(User{})
	nodeTypePtr := GetNodeType(&User{})
	nodeTypeSlice := GetNodeType([]User{})
	nodeTypeSlicePtr := GetNodeType([]*User{})

	assert.Equal(t, "User", nodeTypeStruct)
	assert.Equal(t, "User", nodeTypePtr)
	assert.Equal(t, "User", nodeTypeSlice)
	assert.Equal(t, "User", nodeTypeSlicePtr)
}

func TestCreateSchema(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	firstSchema, err := CreateSchema(c, &User{})
	if err != nil {
		t.Error(err)
	}
	assert.Len(t, firstSchema.Schema, 24)
	assert.Len(t, firstSchema.Types, 2)

	// Create a test logger to capture logs, which will
	// warn about conflicts of username and email
	logSink := newTestLogSink()
	testLogger := logr.New(logSink)
	originalLogger := Logger()
	SetLogger(testLogger)
	defer SetLogger(originalLogger)

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
	assert.Len(t, firstSchema.Types, 2)

	// Verify we captured schema conflict logs
	logs := logSink.GetLogs()
	assert.Greater(t, len(logs), 0, "Should have captured log messages")
	conflictFound := false
	for _, msg := range logs {
		if strings.Contains(msg, "schema conflict") {
			conflictFound = true
			break
		}
	}
	assert.True(t, conflictFound, "Should have logged schema conflicts")
}

func TestMutateSchema(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	firstSchema, err := CreateSchema(c, &User{})
	if err != nil {
		t.Error(err)
	}
	assert.Len(t, firstSchema.Schema, 24)
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
			assert.Equal(t, s.String(), schema.String())
		}
	}
}

func TestOneToOneSchema(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	schema, err := CreateSchema(c, &OneToOne{})
	if err != nil {
		t.Error(err)
	}
	assert.Len(t, schema.Schema, 3)
}

func Test_fetchExistingSchema(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	schema, err := CreateSchema(c, &User{})
	if err != nil {
		t.Error(err)
	}

	existing, err := fetchExistingSchema(c)
	if err != nil {
		t.Error(err)
	}

	for _, existingSchema := range existing {
		if s, ok := schema.Schema[existingSchema.Predicate]; ok {
			assert.Equal(t, s.String(), existingSchema.String())
		}
	}
}

func Test_fetchExistingTypes(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	schema, err := CreateSchema(c, &User{})
	if err != nil {
		t.Error(err)
	}

	types, err := fetchExistingTypes(c, schema.Types)
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, types, 2)
}

func Test_GetSchema(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	type SmallStruct struct {
		UID   string   `json:"uid,omitempty"`
		Name  string   `json:"name,omitempty" dgraph:"index=term"`
		DType []string `json:"dgraph.type"`
	}
	_, err := CreateSchema(c, &SmallStruct{})
	if err != nil {
		t.Error(err)
	}

	schema, err := GetSchema(c)
	if err != nil {
		t.Error(err)
	}

	assert.Contains(t, schema, "name: string @index(term) .")
	assert.Contains(t, schema, "type SmallStruct {\n\tname\n}")
}

func TestMissingDType(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	type SchemaTest struct {
		UID  string `json:"uid,omitempty"`
		Name string `json:"name,omitempty"`
	}
	_, err := CreateSchema(c, &SchemaTest{})
	require.Error(t, err, "expected error due to missing required field DType []string `json:\"dgraph.type\"` in type SchemaTest")
}

func TestParseStructTag_Comprehensive(t *testing.T) {
	tests := []struct {
		tag     string
		expects rawSchema
		comment string
	}{
		{
			tag:     "index=term unique upsert reverse count list lang noconflict predicate=my_predicate type=int",
			expects: rawSchema{Index: "term", Unique: true, Upsert: true, Reverse: true, Count: true, List: true, Lang: true, Noconflict: true, Predicate: "my_predicate", Type: "int"},
			comment: "All boolean and value tokens",
		},
		{
			tag:     "index=hash",
			expects: rawSchema{Index: "hash"},
			comment: "Simple index",
		},
		{
			tag:     "index=hnsw(metric:\"cosine\")",
			expects: rawSchema{Index: "hnsw(metric:\"cosine\")"},
			comment: "HNSW with metric",
		},
		{
			tag:     "index=hnsw(metric:\"euclidean\",exponent:\"6\")",
			expects: rawSchema{Index: "hnsw(metric:\"euclidean\",exponent:\"6\")"},
			comment: "HNSW with metric and exponent",
		},
		{
			tag:     "reverse",
			expects: rawSchema{Reverse: true},
			comment: "Single boolean flag",
		},
		{
			tag:     "predicate=foo type=uid",
			expects: rawSchema{Predicate: "foo", Type: "uid"},
			comment: "Predicate and type override",
		},
	}

	for _, tt := range tests {
		schema, err := parseStructTag(tt.tag)
		assert.NoError(t, err, tt.comment)
		assert.Equal(t, tt.expects.Index, schema.Index, tt.comment+": index")
		assert.Equal(t, tt.expects.Unique, schema.Unique, tt.comment+": unique")
		assert.Equal(t, tt.expects.Upsert, schema.Upsert, tt.comment+": upsert")
		assert.Equal(t, tt.expects.Reverse, schema.Reverse, tt.comment+": reverse")
		assert.Equal(t, tt.expects.Count, schema.Count, tt.comment+": count")
		assert.Equal(t, tt.expects.List, schema.List, tt.comment+": list")
		assert.Equal(t, tt.expects.Lang, schema.Lang, tt.comment+": lang")
		assert.Equal(t, tt.expects.Noconflict, schema.Noconflict, tt.comment+": noconflict")
		assert.Equal(t, tt.expects.Predicate, schema.Predicate, tt.comment+": predicate")
		assert.Equal(t, tt.expects.Type, schema.Type, tt.comment+": type")
	}
}

// testLogSink implements logr.LogSink for testing
type testLogSink struct {
	logs  []string
	mutex sync.Mutex
}

func newTestLogSink() *testLogSink {
	return &testLogSink{logs: make([]string, 0)}
}

// Capture logs any message passed to it
func (l *testLogSink) capture(msg string) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.logs = append(l.logs, msg)
}

// GetLogs returns all captured logs
func (l *testLogSink) GetLogs() []string {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.logs
}

// Implementation of logr.LogSink interface
func (l *testLogSink) Init(logr.RuntimeInfo)  {}
func (l *testLogSink) Enabled(level int) bool { return true }
func (l *testLogSink) Info(level int, msg string, kvs ...interface{}) {
	l.capture(msg)
}
func (l *testLogSink) Error(err error, msg string, kvs ...interface{}) {
	l.capture(msg)
}
func (l *testLogSink) WithValues(kvs ...interface{}) logr.LogSink { return l }
func (l *testLogSink) WithName(name string) logr.LogSink          { return l }
