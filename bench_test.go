/*
 * Copyright (C) 2020 Dolan and Contributors
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

import "testing"

type FlatStruct struct {
	UID    string   `json:"uid,omitempty"`
	Field1 string   `json:"field1,omitempty"`
	Field2 int      `json:"field2,omitempty"`
	Field3 bool     `json:"field3,omitempty"`
	Field4 []int    `json:"field4,omitempty"`
	Field5 []string `json:"field5,omitempty"`
	DType  []string `json:"dgraph.type"`
}

func createFlatStruct() FlatStruct {
	return FlatStruct{
		Field1: "field 1",
		Field2: 2,
		Field3: true,
		Field4: []int{1, 2, 3, 4},
		Field5: []string{"test data 1", "test data 2", "test data 3", "test data 4"},
	}
}

func BenchmarkMutateBasic(b *testing.B) {
	c := newDgraphClient()
	CreateSchema(c, FlatStruct{})
	defer dropAll(c)

	for n := 0; n < b.N; n++ {
		data := createFlatStruct()

		tx := NewTxn(c).SetCommitNow()
		tx.MutateBasic(&data)
	}
}

func BenchmarkMutate(b *testing.B) {
	c := newDgraphClient()
	CreateSchema(c, FlatStruct{})
	defer dropAll(c)

	for n := 0; n < b.N; n++ {
		data := createTestUser()

		tx := NewTxn(c).SetCommitNow()
		tx.Mutate(&data)
	}
}

func BenchmarkMutateBasicNested(b *testing.B) {
	c := newDgraphClient()
	CreateSchema(c, TestUser{})
	defer dropAll(c)

	for n := 0; n < b.N; n++ {
		data := createTestUser()

		tx := NewTxn(c).SetCommitNow()
		tx.MutateBasic(&data)
	}
}

func BenchmarkMutateNested(b *testing.B) {
	c := newDgraphClient()
	CreateSchema(c, TestUser{})
	defer dropAll(c)

	for n := 0; n < b.N; n++ {
		data := createTestUser()

		tx := NewTxn(c).SetCommitNow()
		tx.Mutate(&data)
	}
}
