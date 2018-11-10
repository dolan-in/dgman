package dgman

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type EnumType int

type GeoLoc struct {
	Type  string    `json:"type"`
	Coord []float64 `json:"coordinates"`
}

func (e EnumType) ScalarType() string {
	return "int"
}

type User struct {
	UID      string     `json:"uid,omitempty"`
	Name     string     `json:"name,omitempty" dgraph:"index=term"`
	Username string     `json:"username,omitempty" dgraph:"index=hash"`
	Email    string     `json:"email,omitempty" dgraph:"index=hash upsert"`
	Password string     `json:"password,omitempty"`
	Height   *int       `json:"height,omitempty"`
	Dob      *time.Time `json:"dob,omitempty"`
	Status   EnumType   `json:"status,omitempty"`
	Created  time.Time  `json:"created,omitempty"`
	Mobiles  []string   `json:"mobiles,omitempty"`
	Schools  []School   `json:"schools,omitempty" dgraph:"count reverse"`
	*Anonymous
}

type Anonymous struct {
	Field1 string `json:"field_1,omitempty"`
	Field2 string `json:"field_2,omitempty"`
}

type School struct {
	UID      string `json:"uid,omitempty"`
	Name     string `json:"name,omitempty"`
	Location GeoLoc `json:"location,omitempty" dgraph:"type=geo"` // test passing type
}

type NewUser struct {
	UID      string `json:"uid,omitempty"`
	Username string `json:"username,omitempty" dgraph:"index=term"`
	Email    string `json:"email,omitempty" dgraph:"index=term"`
	Password string `json:"password,omitempty"`
}

func TestMarshalSchema(t *testing.T) {
	schema := marshalSchema(nil, User{})
	assert.Len(t, schema, 13)
	assert.Contains(t, schema, "user")
	assert.Contains(t, schema, "school")
	assert.Contains(t, schema, "username")
	assert.Contains(t, schema, "email")
	assert.Contains(t, schema, "password")
	assert.Contains(t, schema, "name")
	assert.Contains(t, schema, "height")
	assert.Contains(t, schema, "mobiles")
	assert.Contains(t, schema, "status")
	assert.Contains(t, schema, "dob")
	assert.Contains(t, schema, "created")
	assert.Contains(t, schema, "location")
	assert.Contains(t, schema, "schools")
	assert.Equal(t, "username: string @index(hash) .", schema["username"].String())
	assert.Equal(t, "email: string @index(hash) @upsert .", schema["email"].String())
	assert.Equal(t, "password: string .", schema["password"].String())
	assert.Equal(t, "name: string @index(term) .", schema["name"].String())
	assert.Equal(t, "mobiles: [string] .", schema["mobiles"].String())
	assert.Equal(t, "schools: uid @count @reverse .", schema["schools"].String())
	assert.Equal(t, "school: string .", schema["school"].String())
	assert.Equal(t, "status: int .", schema["status"].String())
	assert.Equal(t, "height: int .", schema["height"].String())
	assert.Equal(t, "dob: dateTime .", schema["dob"].String())
	assert.Equal(t, "created: dateTime .", schema["created"].String())
	assert.Equal(t, "user: string .", schema["user"].String())
	assert.Equal(t, "location: geo .", schema["location"].String())
}

func TestCreateSchema(t *testing.T) {
	c := newDgraphClient()
	defer dropAll(c)
	// make sure empty first, so no conflicts
	conflict, err := CreateSchema(c, &User{})
	if err != nil {
		t.Error(err)
	}
	assert.Empty(t, conflict)

	conflict, err = CreateSchema(c, &NewUser{})
	if err != nil {
		t.Error(err)
	}
	// should return conflicts for username and email
	assert.Len(t, conflict, 2)
}
