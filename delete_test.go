package dgman

import (
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

	tx := NewTxn(c)

	err := tx.Create(&users)
	if err != nil {
		t.Error(err)
	}
	if err := tx.Commit(); err != nil {
		t.Error(err)
	}

	_, err = NewTxn(c).Delete(&User{}, true).
		Filter(`allofterms(name, "wildan")`).
		Nodes()
	if err != nil {
		t.Error(err)
	}

	var all []*User
	if err := NewTxn(c).Get(&all).All().Nodes(); err != nil {
		t.Error(err)
	}

	assert.Len(t, all, 1)
}

func TestDeleteQuery(t *testing.T) {
	c := newDgraphClient()
	if _, err := CreateSchema(c, &User{}); err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	school := School{
		Name: "wildan's school",
	}
	tx := NewTxn(c)

	err := tx.Create(&school, true)
	if err != nil {
		t.Error(err)
	}

	users := []*User{
		&User{
			Name:     "wildan",
			Username: "wildan",
			Email:    "wildan2711@gmail.com",
			Schools:  []School{school},
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

	tx = NewTxn(c)

	err = tx.Create(&users)
	if err != nil {
		t.Error(err)
	}
	if err := tx.Commit(); err != nil {
		t.Error(err)
	}
	log.Println(users[0])

	nodes, err := NewTxn(c).Delete(&User{}, true).
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
	if err := NewTxn(c).Get(&all).All().Nodes(); err != nil {
		t.Error(err)
	}

	assert.Len(t, all, 1)
}

func TestDeleteQueryNode(t *testing.T) {
	c := newDgraphClient()
	if _, err := CreateSchema(c, &User{}); err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	school := School{
		Name: "wildan's school",
	}
	tx := NewTxn(c)

	err := tx.Create(&school, true)
	if err != nil {
		t.Error(err)
	}

	users := []*User{
		&User{
			Name:     "wildan",
			Username: "wildan",
			Email:    "wildan2711@gmail.com",
			Schools:  []School{school},
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

	tx = NewTxn(c)

	err = tx.Create(&users)
	if err != nil {
		t.Error(err)
	}
	if err := tx.Commit(); err != nil {
		t.Error(err)
	}
	log.Println(users[0])

	nodes, err := NewTxn(c).Delete(&User{}, true).
		Query(`@filter(eq(email, "wildan2711@gmail.com")) {
			uid
			schools {
				uid
			}
		}`).
		Node()
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, nodes, 2)

	var all []*User
	if err := NewTxn(c).Get(&all).All().Nodes(); err != nil {
		t.Error(err)
	}

	assert.Len(t, all, 2)
}

func TestDeleteEdge(t *testing.T) {
	c := newDgraphClient()
	if _, err := CreateSchema(c, &School{}, &User{}); err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	schools := []School{
		School{
			Name: "wildan's school",
		},
		School{
			Name: "wildan's second school",
		},
		School{
			Name: "wildan's third school",
		},
		School{
			Name: "wildan's fourth school",
		},
	}

	tx := NewTxn(c)

	err := tx.Create(&schools, true)
	if err != nil {
		t.Error(err)
	}

	user := User{
		Name:     "wildan",
		Username: "wildan",
		Email:    "wildan2711@gmail.com",
		Schools:  schools,
	}

	err = NewTxn(c).Create(&user, true)
	if err != nil {
		t.Error(err)
	}

	user = User{}
	err = NewTxn(c).Get(&user).
		All(1).
		Node()
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, user.Schools, 4)

	err = NewTxn(c).Delete(&user, true).
		Edge(user.UID, "schools", user.Schools[0].UID)
	if err != nil {
		t.Error(err)
	}

	user = User{}
	err = NewTxn(c).Get(&user).
		All(1).
		Node()
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, user.Schools, 3)

	err = NewTxn(c).Delete(&user, true).
		Edge(user.UID, "schools")
	if err != nil {
		t.Error(err)
	}

	user = User{}
	err = NewTxn(c).Get(&user).
		All(1).
		Node()
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, user.Schools, 0)
}
