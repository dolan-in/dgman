package dgman

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type User struct {
	UID      string `json:"uid,omitempty"`
	Username string `json:"username,omitempty" dgraph:"index=hash"`
	Email    string `json:"email,omitempty" dgraph:"index=hash"`
	Password string `json:"password,omitempty"`
}

type NewUser struct {
	UID      string `json:"uid,omitempty"`
	Username string `json:"username,omitempty" dgraph:"index=term"`
	Email    string `json:"email,omitempty" dgraph:"index=term"`
	Password string `json:"password,omitempty"`
}

func TestMarshalSchema(t *testing.T) {
	schema := marshalSchema(User{})
	assert.Len(t, schema, 3)
	assert.Contains(t, schema, "username")
	assert.Contains(t, schema, "email")
	assert.Contains(t, schema, "password")
	assert.Equal(t, "username: string @index(hash) .", schema["username"].String())
	assert.Equal(t, "email: string @index(hash) .", schema["email"].String())
	assert.Equal(t, "password: string .", schema["password"].String())
}

func TestFetchSchema(t *testing.T) {
	c := newDgraphClient()
	// make sure empty first, so no conflicts
	conflict, err := CreateSchema(c, "localhost:8080", &User{})
	if err != nil {
		t.Error(err)
	}
	assert.Empty(t, conflict)

	conflict, err = CreateSchema(c, "localhost:8080", &NewUser{})
	if err != nil {
		t.Error(err)
	}
	// should return conflicts for username and email
	assert.Len(t, conflict, 2)

	dropAll(c)
}
