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

	tx := NewTxn(c).SetCommitNow()
	user := createTestUser()

	uids, err := tx.Mutate(&user)
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, uids, 9)

	tx = NewTxn(c).SetCommitNow()
	err = tx.Delete(&DeleteParams{
		Nodes: []DeleteNode{
			// delete the edge
			{
				UID: user.UID,
				Edges: []DeleteEdge{
					{
						Pred: "schools",
						UIDs: []string{user.Schools[0].UID},
					},
				},
			},
			// delete the node
			{
				UID: user.Schools[0].UID,
			},
		},
	})

	tx = NewReadOnlyTxn(c)

	// school node should be deleted
	var school TestSchool
	err = tx.Get(&school).
		UID(user.Schools[0].UID).
		Node()

	assert.Equal(t, ErrNodeNotFound, err)

	var updatedUser TestUser
	err = tx.Get(&updatedUser).
		UID(user.UID).
		All(3).
		Node()
	if err != nil {
		t.Error(err)
	}

	// first school should be deleted
	user.Schools = user.Schools[1:]

	assert.Equal(t, user, updatedUser)

	// delete all school edges
	tx = NewTxn(c).SetCommitNow()
	err = tx.Delete(&DeleteParams{
		Nodes: []DeleteNode{
			// delete the edge
			{
				UID: user.UID,
				Edges: []DeleteEdge{
					{
						Pred: "schools",
					},
				},
			},
			// delete the node
			{
				UID: user.Schools[0].UID,
			},
		},
	})

	var updatedUser2 TestUser
	tx = NewReadOnlyTxn(c)
	err = tx.Get(&updatedUser2).
		UID(user.UID).
		All(3).
		Node()
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, updatedUser2.Schools, 0)
}

func TestDeleteQuery(t *testing.T) {
	c := newDgraphClient()

	_, err := CreateSchema(c, TestUser{})
	if err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	tx := NewTxn(c).SetCommitNow()
	user := createTestUser()

	uids, err := tx.Mutate(&user)
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, uids, 9)

	queryUser := TestUser{}

	tx = NewTxn(c).SetCommitNow()
	query := NewQueryBlock(NewQuery().
		Model(&queryUser).
		UID(user.UID).
		Query(`{
			schools @filter(eq(identifier, "harvard")) {
				schoolId as uid
			}
		}`))
	result, err := tx.DeleteQuery(query, &DeleteParams{
		Nodes: []DeleteNode{
			{
				UID: user.UID,
				Edges: []DeleteEdge{
					{
						Pred: "schools",
						UIDs: []string{"schoolId"},
					},
				},
			},
			{
				UID: "schoolId",
			},
		},
	})
	if err != nil {
		t.Error(err)
	}

	err = result.Scan()
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, queryUser.Schools, 1)

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

	var school TestSchool
	err = tx.Get(&school).
		UID(queryUser.Schools[0].UID).
		Node()

	assert.Equal(t, ErrNodeNotFound, err)
}

func TestDeleteQueryCond(t *testing.T) {
	c := newDgraphClient()

	_, err := CreateSchema(c, TestUser{})
	if err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	tx := NewTxn(c).SetCommitNow()
	user := createTestUser()

	uids, err := tx.Mutate(&user)
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, uids, 9)

	queryUser := TestUser{}

	tx = NewTxn(c).SetCommitNow()
	query := NewQueryBlock(NewQuery().
		Model(&queryUser).
		UID(user.UID).
		Query(`{
			schools @filter(eq(identifier, "harvard")) {
				schoolId as uid
			}
		}`))
	result, err := tx.DeleteQuery(query, &DeleteParams{
		Cond: "@if(gt(len(schoolId), 0))",
		Nodes: []DeleteNode{
			{UID: user.UID},
		},
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

func TestDeleteQueryCondUidFunc(t *testing.T) {
	log.SetFlags(log.Llongfile)
	c := newDgraphClient()

	_, err := CreateSchema(c, TestUser{})
	if err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	tx := NewTxn(c).SetCommitNow()
	user := createTestUser()

	uids, err := tx.Mutate(&user)
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, uids, 9)

	tx = NewReadOnlyTxn(c)

	schools := []TestSchool{}
	myQuery := tx.Get(&schools).Filter("uid($1)", UIDs([]string{user.Schools[0].UID, user.Schools[1].UID}))
	err = myQuery.Nodes()
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, schools, 2)

	queryUser := TestUser{}

	tx = NewTxn(c).SetCommitNow()
	query := NewQueryBlock(NewQuery().
		Model(&queryUser).
		UID(user.UID).
		Query(`{
			schools {
				schoolId as uid
			}
		}`))
	result, err := tx.DeleteQuery(query, &DeleteParams{
		Cond: "@if(gt(len(schoolId), 1))",
		Nodes: []DeleteNode{
			{UID: "schoolId"},
		},
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

	tx = NewTxn(c).SetCommitNow()
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

	tx := NewTxn(c).SetCommitNow()
	user := createTestUser()

	uids, err := tx.Mutate(&user)
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, uids, 9)

	tx = NewTxn(c).SetCommitNow()
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

func TestDeleteEdge(t *testing.T) {
	c := newDgraphClient()

	_, err := CreateSchema(c, TestUser{})
	if err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	tx := NewTxn(c).SetCommitNow()
	user := createTestUser()

	uids, err := tx.Mutate(&user)
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, uids, 9)

	tx = NewTxn(c).SetCommitNow()
	if err = tx.DeleteEdge(user.UID, "schools", user.Schools[0].UID); err != nil {
		t.Error(err)
	}

	tx = NewReadOnlyTxn(c)

	var updatedUser TestUser
	err = tx.Get(&updatedUser).
		UID(user.UID).
		All(3).
		Node()

	assert.Len(t, updatedUser.Schools, 1)
	assert.Equal(t, updatedUser.Schools[0].UID, user.Schools[1].UID)
}
