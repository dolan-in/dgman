
[![Build Status](https://travis-ci.com/dolan-in/dgman.svg?branch=master)](https://travis-ci.com/dolan-in/dgman)
[![Coverage Status](https://coveralls.io/repos/github/dolan-in/dgman/badge.svg?branch=master)](https://coveralls.io/github/dolan-in/dgman?branch=master)
[![GoDoc](https://godoc.org/github.com/dolan-in/dgman?status.svg)](https://godoc.org/github.com/dolan-in/dgman)

***Dgman*** is a schema manager for [Dgraph](https://dgraph.io/) using the [Go Dgraph client (dgo)](https://github.com/dgraph-io/dgo), which manages Dgraph schema and indexes from Go tags in struct definitions

## Features
- Create schemas and indexes from struct tags.
- Detect conflicts from existing schema and defined schema.
- Mutate Helpers (Create, Update)
- Autoinject [node type](https://docs.dgraph.io/howto/#giving-nodes-a-type) from struct.
- Field unique checking (e.g: emails, username).
- Query helpers.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
  - [Schema Definition](#schema-definition)
    - [CreateSchema](#createschema)
    - [MutateSchema](#mutateschema)
  - [Mutate Helpers](#mutate-helpers)
    - [Mutate](#mutate)
    - [Node Types](#node-types)
    - [Create (Mutate with Unique Checking)](#create-mutate-with-unique-checking)
    - [Update (Mutate Existing Node with Unique Checking)](#update-mutate-existing-node-with-unique-checking)
  - [Query Helpers](#query-helpers)
    - [Get by UID](#get-by-uid)
    - [Get by Filter](#get-by-filter)
    - [Get by Query](#get-by-query)
  - [Delete Helper](#delete-helper)

## Installation

Using go get:

`go get github.com/dolan-in/dgman`

## Usage 

```
import(
	"github.com/dolan-in/dgman"
)
```

### Schema Definition

Schemas are defined using Go structs which defines the predicate name from the `json` tag, indices and directives using the `dgraph` tag.

#### CreateSchema

Using the `CreateSchema` function, it will install the schema, and detect schema and index conflicts within the passed structs and with the currently existing schema in the specified Dgraph database.

```go
// User is a node, nodes have a uid field
type User struct {
	UID      string     `json:"uid,omitempty"`
	Name     string     `json:"name,omitempty" dgraph:"index=term"` // use term index 
	Username string     `json:"username,omitempty" dgraph:"index=hash"` // use hash index
	Email    string     `json:"email,omitempty" dgraph:"index=hash upsert"` // use hash index, use upsert directive
	Password string     `json:"password,omitempty"`
	Height   *int       `json:"height,omitempty"`
	Dob      *time.Time `json:"dob,omitempty"` // will be inferred as dateTime schema type
	Status   EnumType   `json:"status,omitempty"`
	Created  time.Time  `json:"created,omitempty" dgraph:"index=day"` // will be inferred as dateTime schema type, with day index
	Mobiles  []string   `json:"mobiles,omitempty"` // will be inferred as using the  [string] schema type, slices with primitive types will all be inferred as lists
	Schools  []School   `json:"schools,omitempty" dgraph:"count reverse"` // defines an edge to other nodes, add count index, add reverse edges
}

// School is another node, that will be connected to User node using the schools predicate
type School struct {
	UID      string `json:"uid,omitempty"`
	Name     string `json:"name,omitempty"`
	Location GeoLoc `json:"location,omitempty" dgraph:"type=geo"` // for geo schema type, need to specify explicitly
}


// If custom types are used, you need to specify the type in the ScalarType() method
type EnumType int

func (e EnumType) ScalarType() string {
	return "int"
}

type GeoLoc struct {
	Type  string    `json:"type"`
	Coord []float64 `json:"coordinates"`
}

func main() {
	d, err := grpc.Dial("localhost:9080", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	c := dgo.NewDgraphClient(api.NewDgraphClient(d))

	// create the schema, 
	// it will only install non-existing schema in the specified database
	schema, err := dgman.CreateSchema(c, &User{})
	if err != nil {
		panic(err)
	}
	// Check the generated schema
	fmt.Println(schema)
}

```

On an empty database, the above code will return the generated schema string used to create the schema, logging the conflicting schemas in the process:

```
2018/12/14 02:23:48 conflicting schema name, already defined as "name: string @index(term) .", trying to define "name: string ."
username: string @index(hash) .
status: int .
created: dateTime @index(day) .
mobiles: [string] .
schools: uid @count @reverse .
user: string .
email: string @index(hash) @upsert .
password: string .
height: int .
dob: dateTime .
school: string .
location: geo .
name: string @index(term) .
```

When schema conflicts is detected with the existing schema already installed in the database, it will only log the differences. You would need to manually correct the conflicts by dropping or updating the schema manually.

#### MutateSchema

To overwrite/update index definitions, you can use the `MutateSchema` function, which will update the schema indexes.

	// update the schema indexes
	schema, err := dgman.MutateSchema(c, &User{})
	if err != nil {
		panic(err)
	}
	// Check the generated schema
	fmt.Println(schema)

### Mutate Helpers

#### Mutate

Using the `Mutate` function, before sending a mutation, it will marshal a struct into JSON and injecting a [node type](https://docs.dgraph.io/howto/#giving-nodes-a-type), for easier labelling nodes, or in SQL it would refer to the table.

```go
user := User{
	Name: "Alexander",
	Email: "alexander@gmail.com",
	Username: "alex123",
}

if err := dgman.Mutate(context.Background(), c.NewTxn(), &user, dgman.MutateOptions{CommitNow: true}); err != nil {
	panic(err)
}

// UID will be set
fmt.Println(user.UID)
```

The above will insert a node with the following JSON string, with the field `"user":""` added in:

```json
{"user":"","email":"alexander@gmail.com","username":"alex123"}
```

#### Node Types

[Node types](https://docs.dgraph.io/howto/#giving-nodes-a-type) will be inferred from the struct name and converted into snake_case, so the `User` struct above would use `user` as its node type.

If you need to define a custom name for the node type, you can define the `NodeType() string` method on the struct.

```go
type CustomNodeType struct {
	UID 	string `json:"uid,omitempty"`
	Name 	string `json:"name,omitempty"`
}

func (c CustomNodeType) NodeType() string {
	return "node_type"
}
```

#### Create (Mutate with Unique Checking)

If you need unique checking for a particular field of a node with a certain node type, e.g: Email of users, you can use the `Create` function.

To define a field to be unique, add `unique` in the `dgraph` tag on the struct definition.

```go
type User struct {
	UID 		string `json:"uid,omitempty"`
	Name 		string `json:"name,omitempty" dgraph:"index=term"`
	Email 		string `json:"email,omitempty" dgraph:"index=hash unique"`
	Username 	string `json:"username,omitempty" dgraph:"index=term unique"`
}

...
	user := User{
		Name: "Alexander",
		Email: "alexander@gmail.com",
		Username: "alex123",
	}

	if err := dgman.Create(context.Background(), c.NewTxn(), &user, dgman.MutateOptions{CommitNow: true}); err != nil {
		panic(err)
	}
	
	// try to create user with a duplicate email
	duplicateEmail := User{
		Name: "Alexander",
		Email: "alexander@gmail.com",
		Username: "alexa",
	}

	// will return a dgman.UniqueError
	if err := dgman.Create(context.Background(), c.NewTxn(), &duplicateEmail, dgman.MutateOptions{CommitNow: true}); err != nil {
		if uniqueErr, ok := err.(dgman.UniqueError); ok {
			// check the duplicate field
			fmt.Println(uniqueErr.Field, uniqueErr.Value)
		}
	}

```

#### Update (Mutate existing node with Unique Checking)

This is similar to `Create`, but for existing nodes. So the `uid` field must be specified.

```go
type User struct {
	UID 			string 		`json:"uid,omitempty"`
	Name 			string 		`json:"name,omitempty"`
	Email 		string 		`json:"email,omitempty" dgraph:"index=hash unique"`
	Username 	string 		`json:"username,omitempty" dgraph:"index=term unique"`
	Dob				time.Time	`json:"dob" dgraph:"index=day"`
}

...
	users := []User{
		User{
			Name: "Alexander",
			Email: "alexander@gmail.com",
			Username: "alex123",
		},
		User{
			Name: "Fergusso",
			Email: "fergusso@gmail.com",
			Username: "fergusso123",
		},
	}

	if err := dgman.Create(context.Background(), c.NewTxn(), &users, dgman.MutateOptions{CommitNow: true}); err != nil {
		panic(err)
	}
	
	// try to update the user with existing username
	alexander := users[0]
	alexander.Username = "fergusso123"
	// UID should have a value
	fmt.Println(alexander.UID)

	// will return a dgman.UniqueError
	if err := dgman.Update(context.Background(), c.NewTxn(), &alexander, dgman.MutateOptions{CommitNow: true}); err != nil {
		if uniqueErr, ok := err.(dgman.UniqueError); ok {
			// will return duplicate error for username
			fmt.Println(uniqueErr.Field, uniqueErr.Value)
		}
	}

	// try to update the user with non-existing username
	alexander.Username = "wildan"

	if err := dgman.Update(context.Background(), c.NewTxn(), &alexander, dgman.MutateOptions{CommitNow: true}); err != nil {
		panic(err)
	}

	// should be updated
	fmt.Println(alexander)

```

### Query Helpers

#### Get by UID

```go
// Get by UID
user := User{}
if err := dgman.Get(ctx, tx, &user).UID("0x9cd5"); err != nil {
	if err == dgman.ErrNodeNotFound {
		// node not found
	}
}

// struct will be populated if found
fmt.Println(user)
```

### Get by Filter

```go
user := User{}
// get node with node type `user` that matches filter
err := dgman.Get(ctx, tx, &user).
	Vars("getUser($name: string)", map[string]string{"$name": "wildan"}). // function defintion and Graphql variables
	Filter("allofterms(name, $name)"). // dgraph filter
	Node() // get single node from query
if err != nil {
	if err == dgman.ErrNodeNotFound {
		// node using the specified filter not found
	}
}

// struct will be populated if found
fmt.Println(user)
```

### Get by query

```go
users := []User{}
query := `@filter(allofterms(name, $name)) {
	uid
	expand(_all_) {
		uid
		expand(_all_)
	}
}`
// get nodes with node type `user` that matches filter
err := dgman.Get(ctx, tx, &users).
	Vars("getUsers($name: string)", map[string]string{"$name": "wildan"}). // function defintion and Graphql variables
	Query(query). // dgraph query portion (without root function)
	OrderAsc("name"). // ordering ascending by predicate
	OrderDesc("dob"). // multiple ordering is allowed
	First(10). // get first 10 nodes from result
	Nodes() // get all nodes from the prepared query
if err != nil {
}

// slice will be populated if found
fmt.Println(users)
```

## Delete Helper

Delete helpers can be used to simplify deleting nodes that matches a query, using the same query format as [Query Helpers](#query-helpers).

```go
query := `@filter(allofterms(name, $name)) {
	uid
	expand(_all_) {
		uid
	}
}`
// delete all nodes with node type `user` that matches query
// all edge nodes that are specified in the query will also be deleted
deletedUids, err := dgman.Delete(ctx, tx, &User{}, dgman.MutateOptions{CommitNow: true}).
	Vars("getUsers($name: string)", map[string]string{"$name": "wildan"}). // function defintion and Graphql variables
	Query(query). // dgraph query portion (without root function)
	OrderAsc("name"). // ordering ascending by predicate
	OrderDesc("dob"). // multiple ordering is allowed
	First(10). // get first 10 nodes from result
	Nodes() // delete all nodes from the prepared query
if err != nil {
}

// check the deleted uids
fmt.Println(deletedUids)
```
