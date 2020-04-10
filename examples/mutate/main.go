package main

import (
	"context"
	"fmt"
	"time"

	"github.com/dolan-in/dgman"

	"github.com/dgraph-io/dgo/v200"

	"github.com/dgraph-io/dgo/v200/protos/api"
	"google.golang.org/grpc"
)

// User is a node, nodes have a uid field
type User struct {
	UID      string     `json:"uid,omitempty"`
	Name     string     `json:"name,omitempty" dgraph:"index=term"`                   // use term index
	Username string     `json:"username,omitempty" dgraph:"index=hash upsert unique"` // use hash index
	Email    string     `json:"email,omitempty" dgraph:"index=hash upsert unique"`    // use hash index, use upsert directive
	Password string     `json:"password,omitempty"`
	Height   *int       `json:"height,omitempty"`
	Dob      *time.Time `json:"dob,omitempty"` // will be inferred as dateTime schema type
	Status   EnumType   `json:"status,omitempty"`
	Created  time.Time  `json:"created,omitempty" dgraph:"index=day"`     // will be inferred as dateTime schema type, with day index
	Mobiles  []string   `json:"mobiles,omitempty"`                        // will be inferred as using the  [string] schema type, slices with primitive types will all be inferred as lists
	Schools  []School   `json:"schools,omitempty" dgraph:"count reverse"` // defines an edge to other nodes, add count index, add reverse edges
	DType    []string   `json:"dgraph.type,omitempty"`
}

// School is another node, that will be connected to User node using the schools predicate.
type School struct {
	UID      string   `json:"uid,omitempty"`
	Name     string   `json:"name,omitempty"`
	Location GeoLoc   `json:"location,omitempty" dgraph:"type=geo"` // for geo schema type, need to specify explicitly
	DType    []string `json:"dgraph.type,omitempty"`
}

// EnumType If custom types are used, you need to specity the type in the ScalarType() method
type EnumType int

func (e EnumType) ScalarType() string {
	return "int"
}

type GeoLoc struct {
	Type  string    `json:"type"`
	Coord []float64 `json:"coordinates"`
}

func dropAll(c *dgo.Dgraph) {
	err := c.Alter(context.Background(), &api.Operation{DropAll: true})
	if err != nil {
		panic(err)
	}
}

func main() {
	d, err := grpc.Dial("localhost:9080", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	c := dgo.NewDgraphClient(api.NewDgraphClient(d))
	defer dropAll(c)

	// create the schema,
	// it will only install non-existing schema in the specified database
	_, err = dgman.CreateSchema(c, &User{})
	if err != nil {
		panic(err)
	}

	user := User{
		Name:     "Alexander",
		Email:    "alexander@gmail.com",
		Username: "alex123",
	}

	if err := dgman.NewTxn(c).Mutate(&user, true); err != nil {
		panic(err)
	}
	fmt.Println(user.UID)

	user = User{
		Name:     "Alexander",
		Email:    "alexander@gmail.com",
		Username: "alexa",
	}

	if err := dgman.NewTxn(c).Create(&user, true); err != nil {
		if uniqueErr, ok := err.(*dgman.UniqueError); ok {
			// check the duplicate field
			fmt.Println(uniqueErr.Field, uniqueErr.Value)
		}
	}
}
