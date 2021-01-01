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
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestUser struct {
	UID        string        `json:"uid,omitempty"`
	Name       string        `json:"name,omitempty"`
	Username   string        `json:"username,omitempty" dgraph:"index=term unique"`
	Email      string        `json:"email,omitempty" dgraph:"index=term unique"`
	Schools    []TestSchool  `json:"schools,omitempty" dgraph:"count"`
	SchoolsPtr []*TestSchool `json:"schoolsPtr,omitempty" dgraph:"count"`
	School     *TestSchool   `json:"school,omitempty"`
	DType      []string      `json:"dgraph.type,omitempty" dgraph:"User"`
}

type TestSchool struct {
	UID        string        `json:"uid,omitempty"`
	Name       string        `json:"name,omitempty"`
	Identifier string        `json:"identifier,omitempty" dgraph:"index=term unique"`
	EstYear    int           `json:"estYear,omitempty"`
	Location   *TestLocation `json:"location,omitempty"`
	DType      []string      `json:"dgraph.type,omitempty"`
}

type TestSchoolList []TestSchool

func (l TestSchoolList) Len() int { return len(l) }

func (l TestSchoolList) Swap(i, j int) { l[i], l[j] = l[j], l[i] }

type ByUID struct{ TestSchoolList }

func (l ByUID) Less(i, j int) bool { return l.TestSchoolList[i].UID < l.TestSchoolList[j].UID }

type TestLocation struct {
	UID        string   `json:"uid,omitempty"`
	LocationID string   `json:"locationId,omitempty" dgraph:"index=term unique"`
	DType      []string `json:"dgraph.type,omitempty" dgraph:"Location"`
}

func createTestUser() TestUser {
	return TestUser{
		Name:     "wildan",
		Username: "wildan2711",
		Email:    "wildan2711@gmail.com",
		School: &TestSchool{
			Name:       "BSS",
			Identifier: "bss",
			Location: &TestLocation{
				LocationID: "Malang",
			},
			EstYear: 1231,
		},
		SchoolsPtr: []*TestSchool{
			{
				Name:       "lab",
				Identifier: "lab",
				Location: &TestLocation{
					LocationID: "Malangian",
				},
				EstYear: 3131,
			},
		},
		Schools: []TestSchool{
			{
				Name:       "Kensington",
				Identifier: "kensington",
				Location: &TestLocation{
					LocationID: "perth",
				},
				EstYear: 1234,
			},
			{
				Name:       "Harvard",
				Identifier: "harvard",
				Location: &TestLocation{
					LocationID: "New York",
				},
				EstYear: 2013,
			},
		},
	}
}

func TestMutationMutateBasic(t *testing.T) {
	c := newDgraphClient()

	_, err := CreateSchema(c, TestUser{})
	if err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	tx := NewTxn(c).SetCommitNow()
	user := createTestUser()

	uids, err := tx.MutateBasic(&user)
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, uids, 9)
}

func TestMutationMutate(t *testing.T) {
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
	user = createTestUser()

	uids, err = tx.Mutate(&user)
	assert.Len(t, uids, 0)
	assert.IsType(t, &UniqueError{}, err, err.Error())
}

func TestMutationUpdate(t *testing.T) {
	c := newDgraphClient()

	_, err := CreateSchema(c, TestUser{})
	if err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	tx := NewTxn(c).SetCommitNow()
	user := createTestUser()

	uids1, err := tx.Mutate(&user)
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, uids1, 9)

	// Update the fields, after uid has been injected after insert
	tx = NewTxn(c).SetCommitNow()
	user.Name = "Changed man"
	user.School.Name = "Changed School"
	user.Schools[0].Name = "Changed School 0"
	user.Schools[1].Name = "Changed School 1"
	user.SchoolsPtr[0].Name = "Changed School Ptr 1"

	uids2, err := tx.Mutate(&user)
	require.NoError(t, err)

	assert.Len(t, uids2, 0)

	sortByUID := ByUID{TestSchoolList: user.Schools}
	sort.Sort(sortByUID)

	// query the user, check if the user is correctly updated on upsert
	tx = NewReadOnlyTxn(c)
	var updatedUser TestUser
	if err := tx.Get(&updatedUser).UID(user.UID).All(3).Node(); err != nil {
		t.Error(err)
	}

	assert.Equal(t, user, updatedUser)
}

func TestMutationMutateOrGet(t *testing.T) {
	c := newDgraphClient()

	_, err := CreateSchema(c, TestUser{})
	if err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	tx := NewTxn(c).SetCommitNow()
	user1 := createTestUser()

	uids, err := tx.MutateOrGet(&user1)
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, uids, 9)

	// try to create identical nodes from user1
	// should not create any nodes, but return existing nodes
	tx = NewTxn(c).SetCommitNow()
	user2 := createTestUser()
	uids, err = tx.MutateOrGet(&user2)
	require.NoError(t, err)

	sortByUID := ByUID{TestSchoolList: user1.Schools}
	sort.Sort(sortByUID)

	sortByUID = ByUID{TestSchoolList: user2.Schools}
	sort.Sort(sortByUID)

	assert.Len(t, uids, 0)
	assert.Equal(t, user1, user2)

	tx = NewReadOnlyTxn(c)

	var user TestUser
	err = tx.Get(&user).UID(user2.UID).All(3).Node()
	require.NoError(t, err)

	sortByUID = ByUID{TestSchoolList: user.Schools}
	sort.Sort(sortByUID)

	assert.Equal(t, user2, user)
}

func TestMutationMutateOrGetNested(t *testing.T) {
	c := newDgraphClient()

	_, err := CreateSchema(c, TestUser{})
	if err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	tx := NewTxn(c).SetCommitNow()
	user1 := TestUser{
		Name:     "wildan ms",
		Username: "wildan2711",
		Email:    "wildan2711@gmail.com",
		School: &TestSchool{
			Name:       "Harvard University",
			Identifier: "harvard",
		},
	}

	uids, err := tx.MutateOrGet(&user1)
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, uids, 2)

	// create
	tx = NewTxn(c).SetCommitNow()
	user2 := TestUser{
		Name:     "wildan ms",
		Username: "wildancok2711",
		Email:    "wildancok2711@gmail.com",
		School: &TestSchool{
			Name:       "Harvard Uni",
			Identifier: "harvard",
		},
	}
	uids, err = tx.MutateOrGet(&user2)
	require.NoError(t, err)

	assert.Len(t, uids, 1)
	assert.Equal(t, user1.School, user2.School)

	tx = NewReadOnlyTxn(c)

	var user TestUser
	err = tx.Get(&user).UID(user2.UID).All(2).Node()
	require.NoError(t, err)

	assert.Equal(t, user2, user)
}

func TestMutationMutateOrGetMultipleUnique(t *testing.T) {
	c := newDgraphClient()

	_, err := CreateSchema(c, TestUser{})
	if err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	tx := NewTxn(c).SetCommitNow()
	user1 := TestUser{
		Name:     "wildan ms",
		Username: "wildan2711",
		Email:    "wildan2711@gmail.com",
		School: &TestSchool{
			Name:       "Harvard University",
			Identifier: "harvard",
		},
	}

	uids, err := tx.MutateOrGet(&user1)
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, uids, 2)

	// will get existing node
	tx = NewTxn(c).SetCommitNow()
	user2 := TestUser{
		Name:     "wildan ms",
		Username: "wildan2711",
		Email:    "wildancok2711@gmail.com",
		School: &TestSchool{
			Name:       "Kensington",
			Identifier: "kensington",
		},
	}
	uids, err = tx.MutateOrGet(&user2)
	require.NoError(t, err)

	assert.Len(t, uids, 0)
	assert.Equal(t, user1, user2)

	tx = NewReadOnlyTxn(c)

	var user TestUser
	err = tx.Get(&user).UID(user2.UID).All(2).Node()
	require.NoError(t, err)

	assert.Equal(t, user2, user)
}

func TestMutationMutateOrGetMultipleUniqueNested(t *testing.T) {
	c := newDgraphClient()

	_, err := CreateSchema(c, TestUser{})
	if err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	tx := NewTxn(c).SetCommitNow()
	user1 := TestUser{
		Name:     "wildan ms",
		Username: "wildan2711",
		Email:    "wildan2711@gmail.com",
		School: &TestSchool{
			Name:       "Harvard University",
			Identifier: "harvard",
		},
	}

	uids, err := tx.MutateOrGet(&user1)
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, uids, 2)

	// create
	tx = NewTxn(c).SetCommitNow()
	user2 := TestUser{
		Name:     "wildan ms",
		Username: "wildancok2711",
		Email:    "wildancok2711@gmail.com",
		School: &TestSchool{
			Name:       "Harvard uni",
			Identifier: "harvard",
		},
	}
	uids, err = tx.MutateOrGet(&user2)
	require.NoError(t, err)

	assert.Len(t, uids, 1)
	assert.Equal(t, user1.School, user2.School)
}

func TestMutationUpsert(t *testing.T) {
	c := newDgraphClient()

	_, err := CreateSchema(c, TestUser{})
	if err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	tx := NewTxn(c).SetCommitNow()
	user1 := createTestUser()

	uids1, err := tx.Upsert(&user1)
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, uids1, 9)

	// try to create similar nodes from user1, but modified fields on non-unique fields
	// should not create any nodes, but update existing nodes
	tx = NewTxn(c).SetCommitNow()
	user2 := createTestUser()
	user2.Name = "Changed man"
	user2.Email = "wildancok2711@gmail.com"
	user2.School.Name = "Changed School"
	user2.Schools[0].Name = "Changed School 0"
	user2.Schools[1].Name = "Changed School 1"
	user2.SchoolsPtr[0].Name = "Changed School Ptr 1"

	uids2, err := tx.Upsert(&user2, "username")
	require.NoError(t, err)

	assert.Len(t, uids2, 0)

	sortByUID := ByUID{TestSchoolList: user2.Schools}
	sort.Sort(sortByUID)

	// query the user, check if the user is correctly updated on upsert
	tx = NewReadOnlyTxn(c)
	var updatedUser TestUser
	if err := tx.Get(&updatedUser).UID(user2.UID).All(3).Node(); err != nil {
		t.Error(err)
	}

	assert.Equal(t, user2, updatedUser)
}

func TestMutationUpsert_UniqueError(t *testing.T) {
	c := newDgraphClient()

	_, err := CreateSchema(c, TestUser{})
	if err != nil {
		t.Error(err)
	}
	defer dropAll(c)

	tx := NewTxn(c).SetCommitNow()
	user1 := createTestUser()

	uids1, err := tx.Upsert(&user1)
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, uids1, 9)

	// try to create similar nodes from user1, but modified fields on non-unique fields
	// should not create any nodes, but return unique error
	tx = NewTxn(c).SetCommitNow()
	user2 := createTestUser()
	user2.Name = "Changed man"
	user2.School.Name = "Changed School"
	user2.Schools[0].Name = "Changed School 0"
	user2.Schools[1].Name = "Changed School 1"
	user2.SchoolsPtr[0].Name = "Changed School Ptr 1"

	uids2, err := tx.Upsert(&user2)

	assert.IsType(t, &UniqueError{}, err)
	assert.Len(t, uids2, 0)
}

func TestSetTypes(t *testing.T) {
	user := TestUser{
		School: &TestSchool{
			Location: &TestLocation{},
		},
	}

	err := SetTypes(&user)
	require.NoError(t, err)

	assert.Equal(t, "User", user.DType[0])
	assert.Equal(t, "TestSchool", user.School.DType[0])
	assert.Equal(t, "Location", user.School.Location.DType[0])
}
