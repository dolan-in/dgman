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

func (n TestCustomNode) NodeType() string {
	return "custom_node_type"
}

func TestAddNode(t *testing.T) {
	testData := TestNode{"", "test"}

	c := newDgraphClient()
	defer dropAll(c)

	tx := c.NewTxn()
	uid, err := Mutate(context.Background(), tx, &testData)
	if err != nil {
		t.Error(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
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
	assert.Equal(t, uid, result.Data[0].UID)
}

func TestAddCustomeNode(t *testing.T) {
	testData := TestCustomNode{"", "test"}

	c := newDgraphClient()
	defer dropAll(c)

	tx := c.NewTxn()
	uid, err := Mutate(context.Background(), tx, &testData)
	if err != nil {
		t.Error(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
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
	assert.Equal(t, uid, result.Data[0].UID)
}

func TestAddNodeType(t *testing.T) {
	testData := TestNode{"", "test"}
	jsonData, err := marshalAndInjectType(&testData, false)
	if err != nil {
		t.Error(err)
	}

	expected := "{\"test_node\":\"\",\"field\":\"test\"}"
	if string(jsonData) != expected {
		t.Errorf("expected %s got %s", expected, jsonData)
	}
}
