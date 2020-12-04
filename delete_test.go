package dgman

import (
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDelete(t *testing.T) {
	c := newDgraphClient()

	_, err := CreateSchema(c, TestUser{})
	if err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	tx := NewTxn(c).CommitNow()
	user := createTestUser()

	uids, err := tx.Mutate(&user)
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, uids, 9)

	queryUser := TestUser{}

	tx = NewTxn(c)
	query := NewQueryBlock(NewQuery().
		Model(&queryUser).
		UID(user.UID).
		Query(`{
			schools @filter(eq(identifier, "harvard")) {
				schoolId as uid
			}
		}`))
	result, err := tx.Delete(query, "schoolId")
	if err != nil {
		t.Error(err)
	}

	err = result.Scan()
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, queryUser.Schools, 1)

	schoolUids := make([]string, len(queryUser.Schools))
	for i, school := range queryUser.Schools {
		schoolUids[i] = school.UID
	}

	if err = tx.DeleteEdge(user.UID, "schools", schoolUids...); err != nil {
		t.Error(err)
	}

	err = tx.Commit()
	if err != nil {
		t.Error(err)
	}

	tx = NewReadOnlyTxn(c)

	var updatedUser TestUser
	err = tx.Get(&updatedUser).
		UID(user.UID).
		All(3).
		Node()
	if err != nil {
		t.Error(err)
	}

	// school with identifier harvard should be deleted
	user.Schools = user.Schools[:1]

	assert.Equal(t, user, updatedUser)
}

func TestDeleteCond(t *testing.T) {
	c := newDgraphClient()

	_, err := CreateSchema(c, TestUser{})
	if err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	tx := NewTxn(c).CommitNow()
	user := createTestUser()

	uids, err := tx.Mutate(&user)
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, uids, 9)

	queryUser := TestUser{}

	tx = NewTxn(c).CommitNow()
	query := NewQueryBlock(NewQuery().
		Model(&queryUser).
		UID(user.UID).
		Query(`{
			schools @filter(eq(identifier, "harvard")) {
				schoolId as uid
			}
		}`))
	result, err := tx.DeleteCond(query, DeleteCond{
		Cond: "@if(gt(len(schoolId), 0))",
		Uids: []string{user.UID},
	})
	if err != nil {
		t.Error(err)
	}

	err = result.Scan()
	if err != nil {
		t.Error(err)
	}

	schoolUids := make([]string, len(queryUser.Schools))
	for i, school := range queryUser.Schools {
		schoolUids[i] = school.UID
	}

	assert.Len(t, schoolUids, 1)

	tx = NewReadOnlyTxn(c)

	var updatedUser TestUser
	err = tx.Get(&updatedUser).
		UID(user.UID).
		All(3).
		Node()

	assert.Equal(t, ErrNodeNotFound, err)
}

func TestDeleteCondUidFunc(t *testing.T) {
	log.SetFlags(log.Llongfile)
	c := newDgraphClient()

	_, err := CreateSchema(c, TestUser{})
	if err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	tx := NewTxn(c).CommitNow()
	user := createTestUser()

	uids, err := tx.Mutate(&user)
	if err != nil {
		t.Error(err)
	}
	log.Printf("%#v", user)

	assert.Len(t, uids, 9)

	tx = NewReadOnlyTxn(c)

	schools := []TestSchool{}
	myQuery := tx.Get(&schools).Filter("uid($1)", UIDs([]string{user.Schools[0].UID, user.Schools[1].UID}))
	log.Println(myQuery)
	err = myQuery.Nodes()
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, schools, 2)

	queryUser := TestUser{}

	tx = NewTxn(c).CommitNow()
	query := NewQueryBlock(NewQuery().
		Model(&queryUser).
		UID(user.UID).
		Query(`{
			schools {
				schoolId as uid
			}
		}`))
	result, err := tx.DeleteCond(query, DeleteCond{
		Cond: "@if(gt(len(schoolId), 1))",
		Uids: []string{"schoolId"},
	})
	if err != nil {
		t.Error(err)
	}

	err = result.Scan()
	if err != nil {
		t.Error(err)
	}

	schoolUids := make([]string, len(queryUser.Schools))
	for i, school := range queryUser.Schools {
		schoolUids[i] = school.UID
	}

	assert.Len(t, queryUser.Schools, 2)

	tx = NewTxn(c).CommitNow()
	if err = tx.DeleteEdge(user.UID, "schools"); err != nil {
		t.Error(err)
	}

	tx = NewReadOnlyTxn(c)

	var updatedUser TestUser
	err = tx.Get(&updatedUser).
		UID(user.UID).
		All(3).
		Node()

	assert.Len(t, updatedUser.Schools, 0)

	schools = []TestSchool{}
	err = tx.Get(&schools).Filter("uid_in($1)", UIDs([]string{user.Schools[0].UID, user.Schools[1].UID})).Nodes()

	assert.Len(t, schools, 0)
}

func TestDeleteNode(t *testing.T) {
	c := newDgraphClient()

	_, err := CreateSchema(c, TestUser{})
	if err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	tx := NewTxn(c).CommitNow()
	user := createTestUser()

	uids, err := tx.Mutate(&user)
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, uids, 9)

	tx = NewTxn(c).CommitNow()
	if err = tx.DeleteNode(user.UID); err != nil {
		t.Error(err)
	}

	tx = NewReadOnlyTxn(c)

	var updatedUser TestUser
	err = tx.Get(&updatedUser).
		UID(user.UID).
		All(3).
		Node()

	assert.Equal(t, ErrNodeNotFound, err)
}
