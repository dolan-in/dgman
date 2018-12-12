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
	UID      string `json:"uid,omitempty"`
	Name     string `json:"name,omitempty"`
	Username string `json:"username,omitempty" dgraph:"index=term unique"`
	Email    string `json:"email,omitempty" dgraph:"index=term unique"`
	No       int    `json:"no,omitempty" dgraph:"index=int unique"`
}

func (n TestCustomNode) NodeType() string {
	return "custom_node_type"
}

func TestAddNode(t *testing.T) {
	testData := TestNode{"", "test"}

	c := newDgraphClient()
	defer dropAll(c)

	tx := c.NewTxn()

	uids, err := Mutate(context.Background(), tx, testData, MutateOptions{CommitNow: true})
	if err != nil {
		t.Error(err)
	}

	uid := uids["blank-0"]

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
	assert.Equal(t, uid, result.Data[0].UID)
}

func TestAddCustomeNode(t *testing.T) {
	testData := TestCustomNode{"", "test"}

	c := newDgraphClient()
	defer dropAll(c)

	tx := c.NewTxn()
	uids, err := Mutate(context.Background(), tx, &testData, MutateOptions{CommitNow: true})
	if err != nil {
		t.Error(err)
	}
	uid := uids["blank-0"]

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
	assert.Equal(t, uid, result.Data[0].UID)
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
	uniqueFields := getAllUniqueFields(testUnique)
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

	_, err := Create(context.Background(), tx, &testUnique)
	if err != nil {
		t.Error(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Error(err)
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
		_, err := Create(context.Background(), tx, &data)
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
