package dgman

import (
	"context"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTraverseUIDs(t *testing.T) {
	queryResult := []byte(`{"data":[{"uid":"0x12","friends":[{"uid":"0x13","friends":[{"uid":"0x18","friends":[{"uid":"0x19"},{"uid":"0x20"}]}]},{"uid":"0x14"}]},{"uid":"0x15","friends":[{"uid":"0x16"},{"uid":"0x17"}]}]}`)

	var model []map[string]interface{}
	if err := Nodes(queryResult, &model); err != nil {
		t.Error(err)
	}

	var uids []string
	for _, m := range model {
		traverseUIDs(&uids, m)
	}

	assert.Len(t, uids, 9)
}

func TestGenerateUidsJson(t *testing.T) {
	queryResult := []byte(`{"data":[{"uid":"0x12","friends":[{"uid":"0x13","friends":[{"uid":"0x18","friends":[{"uid":"0x19"},{"uid":"0x20"}]}]},{"uid":"0x14"}]},{"uid":"0x15","friends":[{"uid":"0x16"},{"uid":"0x17"}]}]}`)

	var model []map[string]interface{}
	if err := Nodes(queryResult, &model); err != nil {
		t.Error(err)
	}

	var uids []string
	for _, m := range model {
		traverseUIDs(&uids, m)
	}

	assert.Len(t, uids, 9)

	uidsJSON := generateUidsJSON(uids)
	expectedResult := []byte(`[{"uid":"0x12"},{"uid":"0x18"},{"uid":"0x19"},{"uid":"0x20"},{"uid":"0x13"},{"uid":"0x14"},{"uid":"0x15"},{"uid":"0x16"},{"uid":"0x17"}]`)

	assert.Len(t, uidsJSON, len(expectedResult))
}

func TestDeleteFilter(t *testing.T) {
	users := []*User{
		&User{
			Name:     "wildan",
			Username: "wildan",
			Email:    "wildan2711@gmail.com",
		},
		&User{
			Name:     "wildan",
			Username: "wildansyah",
			Email:    "wildansyah2711@gmail.com",
		},
		&User{
			Name:     "aha",
			Username: "wildani",
			Email:    "wildani@gmail.com",
		},
	}

	c := newDgraphClient()
	if _, err := CreateSchema(c, &User{}); err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	tx := c.NewTxn()

	err := Create(context.Background(), tx, &users)
	if err != nil {
		t.Error(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Error(err)
	}

	_, err = Delete(context.Background(), c.NewTxn(), &User{}, MutateOptions{CommitNow: true}).
		Filter(`allofterms(name, "wildan")`).
		Nodes()
	if err != nil {
		t.Error(err)
	}

	var all []*User
	if err := Get(context.Background(), c.NewTxn(), &all).All().Nodes(); err != nil {
		t.Error(err)
	}

	assert.Len(t, all, 1)
}

func TestDeleteQuery(t *testing.T) {
	users := []*User{
		&User{
			Name:     "wildan",
			Username: "wildan",
			Email:    "wildan2711@gmail.com",
			Schools: []School{
				School{
					Name: "wildan's school",
				},
			},
		},
		&User{
			Name:     "wildan",
			Username: "wildansyah",
			Email:    "wildansyah2711@gmail.com",
		},
		&User{
			Name:     "aha",
			Username: "wildani",
			Email:    "wildani@gmail.com",
		},
	}

	c := newDgraphClient()
	if _, err := CreateSchema(c, &User{}); err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	tx := c.NewTxn()

	err := Create(context.Background(), tx, &users)
	if err != nil {
		t.Error(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Error(err)
	}
	log.Println(users[0])

	nodes, err := Delete(context.Background(), c.NewTxn(), &User{}, MutateOptions{CommitNow: true}).
		Query(`@filter(allofterms(name, "wildan")) {
			uid
			schools {
				uid
			}
		}`).
		Nodes()
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, nodes, 3)

	var all []*User
	if err := Get(context.Background(), c.NewTxn(), &all).All().Nodes(); err != nil {
		t.Error(err)
	}

	assert.Len(t, all, 1)
}
