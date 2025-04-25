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

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestFloats struct {
	UID    string     `json:"uid,omitempty"`
	Name   string     `json:"name,omitempty"`
	Amount *big.Float `json:"amount,omitempty" dgraph:"index=bigfloat"`
	DType  []string   `json:"dgraph.type,omitempty"`
}

func TestMutationFloats(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)

	_, err := CreateSchema(c, TestFloats{})
	if err != nil {
		t.Fatal(err)
	}
	defer dropAll(c)

	tx := NewTxn(c).SetCommitNow()
	user := TestFloats{
		Name:   "wildan",
		Amount: big.NewFloat(100.556),
	}

	uids, err := tx.MutateBasic(&user)
	if err != nil {
		t.Fatal(err)
	}
	assert.Len(t, uids, 1)
	require.NotEmpty(t, user.UID)

	tx = NewReadOnlyTxn(c)
	var result TestFloats
	err = tx.Get(&result).UID(user.UID).Node()
	require.NoError(t, err)

	// Compare big.Float values properly using their comparison methods
	expectedAmount := big.NewFloat(100.556)
	// Compare with Cmp method and tolerance
	diff := new(big.Float).Sub(result.Amount, expectedAmount)
	diff.Abs(diff)
	tolerance := big.NewFloat(0.0001)
	assert.True(t, diff.Cmp(tolerance) < 0,
		"Expected %v but got %v, difference: %v",
		expectedAmount.Text('f', 6), result.Amount.Text('f', 6), diff.Text('f', 6))

	// Compare string representations with specific precision
	assert.Equal(t, expectedAmount.Text('f', 3), result.Amount.Text('f', 3),
		"Float values should be equal when comparing with precision of 3 decimal places")
}

// TestItem is a struct that contains a vector field for testing
type TestItem struct {
	UID         string         `json:"uid,omitempty"`
	Name        string         `json:"name,omitempty" dgraph:"index=term"`
	Identifier  string         `json:"identifier,omitempty" dgraph:"index=term unique"`
	Description string         `json:"description,omitempty"`
	Vector      *VectorFloat32 `json:"vector,omitempty" dgraph:"index=hnsw(metric:\"cosine\")"`
	DType       []string       `json:"dgraph.type,omitempty" dgraph:"Item"`
}

func TestVectorMutation(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	// Create schema for the test item with vector field
	schema, err := CreateSchema(c, TestItem{})
	if err != nil {
		t.Fatal(err)
	}

	// Check the schema
	assert.Equal(t, "name: string @index(term) .", schema.Schema["name"].String())
	assert.Equal(t, "identifier: string @index(term,hash) @upsert @unique .", schema.Schema["identifier"].String())
	assert.Equal(t, "description: string .", schema.Schema["description"].String())
	assert.Equal(t, "vector: float32vector @index(hnsw(metric:\"cosine\")) .", schema.Schema["vector"].String())

	// Create and insert a test item
	tx := NewTxn(c).SetCommitNow()
	item := TestItem{
		Name:        "Test Item",
		Identifier:  "test-item-1",
		Description: "This is a test item for vector embeddings",
		Vector:      &VectorFloat32{Values: []float32{0.1, 0.2, 0.3, 0.4, 0.5}},
	}

	uids, err := tx.MutateBasic(&item)
	require.NoError(t, err)
	assert.NotEmpty(t, uids)
	assert.NotEmpty(t, item.UID)

	tx = NewReadOnlyTxn(c)
	var result TestItem
	err = tx.Get(&result).UID(item.UID).Node()
	require.NoError(t, err)

	assert.Equal(t, item.Name, result.Name)
	assert.Equal(t, item.Identifier, result.Identifier)
	assert.Equal(t, item.Description, result.Description)

	assert.Len(t, result.Vector.Values, len(item.Vector.Values))
	for i, val := range item.Vector.Values {
		assert.InDelta(t, val, result.Vector.Values[i], 0.0001)
	}

	// Test updating the vector
	updatedItem := result
	updatedItem.Vector.Values = []float32{0.9, 0.8, 0.7, 0.6, 0.5}

	tx = NewTxn(c).SetCommitNow()
	_, err = tx.MutateBasic(&updatedItem)
	require.NoError(t, err)

	// Query and verify update
	tx = NewReadOnlyTxn(c)
	var updatedResult TestItem
	err = tx.Get(&updatedResult).UID(updatedItem.UID).Node()
	require.NoError(t, err)

	assert.Len(t, updatedResult.Vector.Values, len(updatedItem.Vector.Values))
	for i, val := range updatedItem.Vector.Values {
		assert.InDelta(t, val, updatedResult.Vector.Values[i], 0.0001)
	}
}

func TestVectorMutationEuclidean(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	// Create a slightly different struct for euclidean metric
	type EuclideanItem struct {
		UID        string        `json:"uid,omitempty"`
		Name       string        `json:"name,omitempty" dgraph:"index=term"`
		Identifier string        `json:"identifier,omitempty" dgraph:"index=term unique"`
		Vector     VectorFloat32 `json:"vector" dgraph:"index=hnsw(metric:\"euclidean\", exponent:\"6\")"`
		DType      []string      `json:"dgraph.type,omitempty" dgraph:"EuclideanItem"`
	}

	// Create schema for the euclidean item
	schema, err := CreateSchema(c, EuclideanItem{})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "name: string @index(term) .", schema.Schema["name"].String())
	assert.Equal(t, "identifier: string @index(term,hash) @upsert @unique .", schema.Schema["identifier"].String())
	assert.Equal(t, "vector: float32vector @index(hnsw(metric:\"euclidean\", exponent:\"6\")) .", schema.Schema["vector"].String())
	defer dropAll(c)

	// Create and insert a test item with euclidean metric
	tx := NewTxn(c).SetCommitNow()
	item := EuclideanItem{
		Name:       "Euclidean Item",
		Identifier: "euclidean-item-1",
		Vector: VectorFloat32{
			Values: []float32{1.1, 2.2, 3.3, 4.4, 5.5},
		},
	}

	uids, err := tx.Mutate(&item)
	require.NoError(t, err)
	assert.NotEmpty(t, uids)
	assert.NotEmpty(t, item.UID)

	// Query it back
	tx = NewReadOnlyTxn(c)
	var result EuclideanItem
	err = tx.Get(&result).UID(item.UID).Node()
	require.NoError(t, err)

	// Verify vector values match
	assert.Len(t, result.Vector.Values, len(item.Vector.Values))
	for i, val := range item.Vector.Values {
		assert.InDelta(t, val, result.Vector.Values[i], 0.0001)
	}
}

func TestVectorSimilaritySearch(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	_, err := CreateSchema(c, TestItem{})
	if err != nil {
		t.Fatal(err)
	}

	// Insert several items with different vectors
	items := []TestItem{
		{
			Name:        "Item A",
			Identifier:  "item-a",
			Description: "First vector",
			Vector:      &VectorFloat32{Values: []float32{0.1, 0.2, 0.3, 0.4, 0.5}},
		},
		{
			Name:        "Item B",
			Identifier:  "item-b",
			Description: "Second vector",
			Vector:      &VectorFloat32{Values: []float32{0.5, 0.4, 0.3, 0.2, 0.1}},
		},
		{
			Name:        "Item C",
			Identifier:  "item-c",
			Description: "Third vector",
			Vector:      &VectorFloat32{Values: []float32{1.0, 1.0, 1.0, 1.0, 1.0}},
		},
	}

	tx := NewTxn(c)
	for i := range items {
		_, err := tx.MutateBasic(&items[i])
		require.NoError(t, err)
		assert.NotEmpty(t, items[i].UID)
	}
	err = tx.Commit()
	require.NoError(t, err)

	// Query with a vector close to Item B
	vectorVar := "[0.51, 0.39, 0.29, 0.19, 0.09]"

	var testItem TestItem
	query := NewQuery().Model(&testItem).
		RootFunc("similar_to(vector, 1, $vec)")

	txn2 := NewReadOnlyTxn(c)
	err = txn2.Query(query).Vars("similar_to($vec: string)", map[string]string{"$vec": vectorVar}).Scan()
	require.NoError(t, err)

	assert.Equal(t, "Item B", testItem.Name)
	assert.Equal(t, "item-b", testItem.Identifier)
}
