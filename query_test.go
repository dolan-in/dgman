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

	uids, err := Mutate(ctx, tx, source, MutateOptions{CommitNow: true})
	if err != nil {
		t.Error(err)
	}

	uid := uids["blank-0"]

	dst := &TestModel{}
	tx = c.NewTxn()
	if err := GetByUID(ctx, tx, uid, dst); err != nil {
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

	_, err := Mutate(ctx, tx, source, MutateOptions{CommitNow: true})
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

	_, err := Mutate(ctx, tx, &source)
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
