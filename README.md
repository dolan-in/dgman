
[![Build Status](https://travis-ci.com/dolan-in/dgman.svg?branch=master)](https://travis-ci.com/dolan-in/dgman)
[![Coverage Status](https://coveralls.io/repos/github/dolan-in/dgman/badge.svg?branch=master)](https://coveralls.io/github/dolan-in/dgman?branch=master)
[![GoDoc](https://godoc.org/github.com/dolan-in/dgman?status.svg)](https://godoc.org/github.com/dolan-in/dgman)

***Dgman*** is a schema manager for [Dgraph](https://dgraph.io/) using the [Go Dgraph client (dgo)](https://github.com/dgraph-io/dgo), which manages Dgraph types, schema, and indexes from Go tags in struct definitions, allowing ORM-like convenience for developing Dgraph clients in Go.

## Features
- Create [types](https://docs.dgraph.io/query-language/#type-system) (Dgraph v1.1+), schemas, and indexes from struct tags.
- Detect conflicts from existing schema and defined schema.
- Mutate Helpers (Mutate, MutateOrGet, Upsert).
- Autoinject node type from struct.
- Field unique checking (e.g: emails, username).
- Query helpers.
- Delete helpers (Delete n-quads generator, Delete Query, Delete Node, Delete Edge).

## Roadmap
- Query builder

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
  - [Schema Definition](#schema-definition)
    - [Node Types](#node-types)
    - [CreateSchema](#createschema)
    - [MutateSchema](#mutateschema)
  - [Mutate Helpers](#mutate-helpers)
    - [Mutate](#mutate)
	- [Mutate Or Get](#mutate-or-get)
    - [Upsert](#upsert)
  - [Query Helpers](#query-helpers)
    - [Get by Filter](#get-by-filter)
    - [Get by Query](#get-by-query)
    - [Get by UID](#get-by-uid)
    - [Get and Count](#get-and-count)
	- [Custom Scanning Query Results](#custom-scanning-query-results)
	- [Multiple Query Blocks](#multiple-query-blocks)
  - [Delete Helper](#delete-helper)
	- [Delete](#delete)
	- [Delete Query](#delete-query)
	- [Delete Node](#delete-node)
	- [Delete Edge](#delete-edge)
 - [Development](#development)

## Installation

Using go modules:

`go get -u github.com/dolan-in/dgman/v2`

## Usage 

```
import(
	"github.com/dolan-in/dgman/v2"
)
```

### Schema Definition

Schemas are defined using Go structs which defines the predicate name from the `json` tag, indices and directives using the `dgraph` tag. To define a dgraph node struct, `json` fields `uid` and `dgraph.type` is required.


#### Node Types

[Node types](https://docs.dgraph.io/query-language/#type-system) will be inferred from the struct name.

If you need to define a custom name for the node type, you can define it on the `dgraph` tag on the `dgraph.type` field.

```go
type CustomNodeType struct {
	UID 	string 		`json:"uid,omitempty"`
	Name 	string 		`json:"name,omitempty"`
	DType	[]string 	`json:"dgraph.type" dgraph:"CustomNodeType"`
}
```

#### CreateSchema

Using the `CreateSchema` function, it will install the schema, and detect schema and index conflicts within the passed structs and with the currently existing schema in the specified Dgraph database.

```go
// User is a node, nodes have a uid and a dgraph.type json field
type User struct {
	UID           string     `json:"uid,omitempty"`
	Name          string     `json:"name,omitempty" dgraph:"index=term"`         // use term index
	Username      string     `json:"username,omitempty" dgraph:"index=hash"`     // use hash index
	Email         string     `json:"email,omitempty" dgraph:"index=hash upsert"` // use hash index, use upsert directive
	Password      string     `json:"password,omitempty" dgraph:"type=password"` // password type
	Height        *int       `json:"height,omitempty"`
	Description   string     `json:"description" dgraph:"lang"` // multi language support on predicate
	DescriptionEn string     `json:"description@en"`            // will not be parsed as schema
	Dob           *time.Time `json:"dob,omitempty"`             // will be inferred as dateTime schema type
	Status        EnumType   `json:"status,omitempty" dgraph="type=int"`
	Created       time.Time  `json:"created,omitempty" dgraph:"index=day"`     // will be inferred as dateTime schema type, with day index
	Mobiles       []string   `json:"mobiles,omitempty"`                        // will be inferred as using the  [string] schema type, slices with primitive types will all be inferred as lists
	Schools       []School   `json:"schools,omitempty" dgraph:"count reverse"` // defines an edge to other nodes, add count index, add reverse edges
	DType         []string   `json:"dgraph.type,omitempty"`
}

// School is another node, that will be connected to User node using the schools predicate
type School struct {
	UID      string 	`json:"uid,omitempty"`
	Name     string 	`json:"name,omitempty"`
	Location *GeoLoc 	`json:"location,omitempty" dgraph:"type=geo"` // for geo schema type, need to specify explicitly
	DType    []string   `json:"dgraph.type,omitempty"`
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

On an empty database, the above code will return the generated type and schema string used to create the schema, logging the conflicting schemas in the process:

```
2018/12/14 02:23:48 conflicting schema name, already defined as "name: string @index(term) .", trying to define "name: string ."
status: int .
mobiles: [string] .
email: string @index(hash) @upsert .
password: string .
height: int .
dob: datetime .
schools: [uid] @count @reverse .
name: string @index(term) .
username: string @index(hash) .
created: datetime @index(day) .
location: geo .

type School {
        location
        name
}
type User {
        status
        created
        username
        password
        height
        dob
        name
        email
        mobiles
        schools
}
```

When schema conflicts is detected with the existing schema already installed in the database, it will only log the differences. You would need to manually correct the conflicts by dropping or updating the schema manually. 

This may be useful to prevent unnecessary or unwanted re-indexing of your data.

#### MutateSchema

To overwrite/update index definitions, you can use the `MutateSchema` function, which will update the schema indexes.

```go
	// update the schema indexes
	schema, err := dgman.MutateSchema(c, &User{})
	if err != nil {
		panic(err)
	}
	// Check the generated schema
	fmt.Println(schema)
```

### Mutate Helpers

#### Mutate

Using the `Mutate` function, before sending a mutation, it will marshal a struct into JSON, injecting the Dgraph [node type](https://docs.dgraph.io/query-language/#type-system) ("dgraph.type" predicate), and do unique checking on the specified fields.

If you need unique checking for a particular field of a node with a certain node type, e.g: Email of users, you can add `unique` in the `dgraph` tag on the struct definition.

```go

type User struct {
	UID 			string 		`json:"uid,omitempty"`
	Name 			string 		`json:"name,omitempty" dgraph:"index=term"`
	Email 			string 		`json:"email,omitempty" dgraph:"index=hash unique"`
	Username 		string 		`json:"username,omitempty" dgraph:"index=term unique"`
	DType			[]string	`json:"dgraph.type"`
}
...

	user := User{
		Name: "Alexander",
		Email: "alexander@gmail.com",
		Username: "alex123",
	}

	// Create a transaction with context.Background() as the context
	// can be shorthened to dgman.NewTxn(c)
	tx := dgman.NewTxnContext(context.Background(), c).
		SetCommitNow() // set transaction to CommitNow: true, which will autocommit, leaving the transaction to only can be used once

	uids, err := tx.Mutate(&user)
	if err != nil {
		panic(err)
	}

	// UID will be set
	fmt.Println(user.UID)
	// list of created UIDs
	fmt.Println(uids)

	// try to create user with a duplicate email
	duplicateEmail := User{
		Name: "Alexander",
		Email: "alexander@gmail.com",
		Username: "alexa",
	}

	// will return a dgman.UniqueError
	tx = dgman.NewTxn(c)
	_, err = tx.Mutate(&duplicateEmail)
	if err != nil {
		if uniqueErr, ok := err.(*dgman.UniqueError); ok {
			// check the duplicate field
			fmt.Println(uniqueErr.Field, uniqueErr.Value)
		}
	}
```

The above mutation will result in the following json, with `dgraph.type` automatically injected:

```json
{"name":"Alexander","email":"alexander@gmail.com","username":"alex123","dgraph.type":["User"]}
```

##### Updating a Node

If you want to update an existing node, just set the UID on the struct node data being passed to `Mutate`. It will also do unique checking on predicates set to be unique.


```go
type User struct {
	UID 			string 		`json:"uid,omitempty"`
	Name 			string 		`json:"name,omitempty"`
	Email 			string 		`json:"email,omitempty" dgraph:"index=hash unique"`
	Username 		string 		`json:"username,omitempty" dgraph:"index=term unique"`
	Dob				time.Time	`json:"dob" dgraph:"index=day"`
	DType			[]string	`json:"dgraph.type"`
}

...

	users := []*User{
		{
			Name: "Alexander",
			Email: "alexander@gmail.com",
			Username: "alex123",
		},
		{
			Name: "Fergusso",
			Email: "fergusso@gmail.com",
			Username: "fergusso123",
		},
	}

	tx := dgman.NewTxn(c).SetCommitNow()
	_, err := tx.Mutate(&users)
	if err != nil {
		panic(err)
	}
	
	// try to update the user with existing username
	alexander := users[0]
	alexander.Username = "fergusso123"
	// UID should have a value
	fmt.Println(alexander.UID)

	// will return a dgman.UniqueError
	tx := dgman.NewTxn(c).SetCommitNow()
	_, err = tx.Mutate(&alexander)
	if err != nil {
		if uniqueErr, ok := err.(*dgman.UniqueError); ok {
			// will return duplicate error for username
			fmt.Println(uniqueErr.Field, uniqueErr.Value)
		}
	}

	// try to update the user with non-existing username
	alexander.Username = "wildan"

	tx = dgman.NewTxn(c).SetCommitNow()
	_, err = tx.Mutate(&alexander)
	if err != nil {
		panic(err)
	}

	// should be updated
	fmt.Println(alexander)
```

#### Mutate Or Get

`MutateOrGet` creates a node if a node with the value of a *unique* predicate does not exist, otherwise return the existing node.

```go
	users := []*User{
		{
			Name: "Alexander",
			Email: "alexander@gmail.com",
			Username: "alex123",
		},
		{
			Name: "Fergusso",
			Email: "fergusso@gmail.com",
			Username: "fergusso123",
		},
	}

	tx := dgman.NewTxn(c).SetCommitNow()
	uids, err := tx.MutateOrGet(&users)
	if err != nil {
		panic(err)
	}

	// should create 2 nodes
	assert.Len(t, uids, 2)

	users2 := []*User{
		{
			Name: "Alexander",
			Email: "alexander@gmail.com", // existing email
			Username: "myalex",
		},
		{
			Name: "Fergusso",
			Email: "fergusso@gmail.com", // existing email
			Username: "myfergusso",
		},
	}

	tx = dgman.NewTxn(c).SetCommitNow()
	uids, err = tx.MutateOrGet(&users)
	if err != nil {
		panic(err)
	}

	// should not create any new nodes
	assert.Len(t, uids, 0)
	// should return the existing nodes, identical to "users"
	assert.Equal(t, users, users2)
```

#### Upsert

`Upsert` updates a node if a node with the value of a *unique* predicate, as specified on the 2nd parameter, already exists, otherwise insert the node. If a node has multiple unique predicates on a single node type, when other predicates other than the upsert predicate failed the unique check, it will return a `*dgman.UniqueError`.

```go
type User struct {
	UID 			string 		`json:"uid,omitempty"`
	Name 			string 		`json:"name,omitempty"`
	Email 			string 		`json:"email,omitempty" dgraph:"index=hash unique"`
	Username 		string 		`json:"username,omitempty" dgraph:"index=term unique"`
	Dob				time.Time	`json:"dob" dgraph:"index=day"`
	DType			[]string	`json:"dgraph.type"`
}

...
	users := []*User{
		{
			Name: "Alexander",
			Email: "alexander@gmail.com",
			Username: "alex123",
		},
		{
			Name: "Fergusso",
			Email: "fergusso@gmail.com",
			Username: "fergusso123",
		},
	}

	tx := dgman.NewTxn(c).SetCommitNow()
	// will update if existing node on "username" predicate.
	// on an empty database, it will create all the nodes
	uids, err := tx.Upsert(&users, "username")
	if err != nil {
		panic(err)
	}

	// should return 2 uids on an empty database
	fmt.Println(uids)

	user := User{
		Name: "Alexander Graham Bell",
		Email: "alexander@gmail.com",
		Username: "alexander",
	}

	tx = dgman.NewTxn(c).SetCommitNow()
	// if no upsert predicate is passed, the first unique predicate found will be used
	// in this case, "email" is used as the upsert predicate
	uids, err = tx.Upsert(&user)

	// should be equal
	fmt.Println(users[0].UID == user.UID)
```

### Query Helpers

Queries and Filters can be constructed by using ordinal parameter markers in query or filter strings, for example `$1`, `$2`, which should be safe against injections. Alternatively, you can also pass GraphQL named vars, with the `Query.Vars` method, although you have to manually convert your data into strings.

#### Get by Filter

```go
name := "wildanjing"

tx := dgman.NewReadOnlyTxn(c)

user := User{}
// get node with node type `user` that matches filter
err := tx.Get(&user).
	Filter("allofterms(name, $1)", name). // dgraph filter
	All(2). // returns all predicates, expand on 2 level of edge predicates
	Node() // get single node from query
if err != nil {
	if err == dgman.ErrNodeNotFound {
		// node using the specified filter not found
	}
}

// struct will be populated if found
fmt.Println(user)
```

#### Get by query

Get by query

```go
tx := dgman.NewReadOnlyTxn(c)

users := []User{}
// get nodes with node type `user` that matches filter
err := tx.Get(&users).
	Query(`{
		uid
		name
		friends @filter(allofterms(name, $1)) {
			uid 
			name
		}
		schools @filter(allofterms(name, $2)) {
			uid
			name
		}
	}`, "wildan", "harvard"). // dgraph query portion (without root function)
	OrderAsc("name"). // ordering ascending by predicate
	OrderDesc("dob"). // multiple ordering is allowed
	First(10). // get first 10 nodes from result
	Nodes() // get all nodes from the prepared query
if err != nil {
}

// slice will be populated if found
fmt.Println(users)
```

#### Get by filter query

You can also combine `Filter` with `Query`.

```go
name := "wildanjing"
friendName := "wildancok"
schoolUIDs := []string{"0x123", "0x1f"}

tx := dgman.NewReadOnlyTxn(c)

users := []User{}
// get nodes with node type `user` that matches filter
err := tx.Get(&users).
	Filter("allofterms(name, $1)", name).
	Query(`{
		uid
		name
		friends @filter(name, $1) {
			uid 
			name
		}
		schools @filter(uid_in($2)) {
			uid
			name
		}
	}`, friendName, dgman.UIDs(schoolUIDs)). // UIDs is a helper type to parse list of uids as a parameter
	OrderAsc("name"). // ordering ascending by predicate
	OrderDesc("dob"). // multiple ordering is allowed
	First(10). // get first 10 nodes from result
	Nodes() // get all nodes from the prepared query
if err != nil {
}

// slice will be populated if found
fmt.Println(users)
```

#### Get by UID

```go
// Get by UID
tx := dgman.NewReadOnlyTxn(c)

user := User{}
if err := tx.Get(&user).UID("0x9cd5").Node(); err != nil {
	if err == dgman.ErrNodeNotFound {
		// node not found
	}
}

// struct will be populated if found
fmt.Println(user)
```

#### Get and Count

```go
tx := dgman.NewReadOnlyTxn(c)

users := []*User{}

count, err := tx.Get(&users).
	Filter(`anyofterms(name, "wildan")`).
	First(3).
	Offset(3).
	NodesAndCount()

// count should return total of nodes regardless of pagination
fmt.Println(count)
```

#### Custom Scanning Query results

You can alternatively specify a different destination for your query results, by passing it as a parameter to the `Node` or `Nodes`.

```go
type checkPassword struct {
	Valid `json:"valid"`	
}

result := &checkPassword{}

tx := dgman.NewReadOnlyTxnContext(ctx, s.c)
err := tx.Get(&User{}). // User here is only to specify the node type
	Filter("eq(email, $1)", email).
	Query(`{ valid: checkpwd(password, $1) }`, password).
	Node(result)

fmt.Println(result.Valid)
```

#### Multiple Query Blocks

You can specify [multiple query blocks](https://dgraph.io/docs/query-language/#multiple-query-blocks), by passing multiple `Query` objects into `tx.Query`.

```go
tx := dgman.NewReadOnlyTxn(c)

type pagedResults struct {
	Paged    []*User `json:"paged"`
	PageInfo []struct {
		Total int
	}
}

result := &pagedResults{}

query := tx.
	Query(
		dgman.NewQuery().
			As("result"). // sets a variable name to the root query
			Var(). // sets the query as a var, making it not returned in the results
			Type(&User{}). // sets the node type to query by
			Filter(`anyofterms(name, $name)`),
		dgman.NewQuery().
			Name("paged"). // query block name to be returned in the query
			UID("result"). // uid from var
			First(2).
			Offset(2).
			All(1),
		dgman.NewQuery().
			Name("pageInfo").
			UID("result").
			Query(`{ total: count(uid) }`),
	).
	Vars("getByName($name: string)", map[string]string{"$name": "wildan"}) // GraphQL query variables

if err := query.Scan(&result); err != nil {
	panic(err)
}

// result should be populated
fmt.Println(result)
```

### Delete Helper

#### Delete

`Delete` is a delete helper that receives delete parameter object(s), which will generate Delete `n-quads`.

```go
// example type
type User struct {
	UID 			string 		`json:"uid,omitempty"`
	Name 			string 		`json:"name,omitempty"`
	Email 			string 		`json:"email,omitempty" dgraph:"index=hash unique"`
	Username 		string 		`json:"username,omitempty" dgraph:"index=term unique"`
	Schools			[]School	`json:"schools,omitempty"`
	DType			[]string	`json:"dgraph.type,omitempty"`
}

type School struct {
	UID        string        `json:"uid,omitempty"`
	Name       string        `json:"name,omitempty"`
	Identifier string        `json:"identifier,omitempty" dgraph:"index=term unique"`
	EstYear    int           `json:"estYear,omitempty"`
	Location   *TestLocation `json:"location,omitempty"`
	DType      []string      `json:"dgraph.type,omitempty"`
}
...

	userUID := "0x12"
	schoolUID := "0xff"

	tx := NewTxn(c).SetCommitNow()
	err := tx.Delete(&DeleteParams{
		Nodes: []DeleteNode{
			// delete the edge
			{
				UID: userUID,
				Edges: []DeleteEdge{
					{
						Pred: "schools",
						UIDs: []string{schoolUID},
					},
				},
			},
			// delete the node
			{
				UID: schoolUID,
			},
		},
	}
```

#### Delete Query

`DeleteQuery` is a delete helper that receives a query block for querying nodes to be deleted and delete parameter object(s) that corresponds to the query. A condition can be passed on the delete parameter object to define a condition for deleting the node(s) by the query.

```go
	// hypothetical existing user node UID to delete
	userUID := "0x12"

	queryUser := User{}

	tx = NewTxn(c).SetCommitNow()
	// query for delete
	// example case: delete user with uid=0x12 if the schools edge has nodes with predicate identifier="harvard"
	query := NewQueryBlock(NewQuery().
		Model(&queryUser).
		UID(user.UID).
		Query(`{
			schools @filter(eq(identifier, "harvard")) {
				schoolId as uid
			}
		}`))
	result, err := tx.DeleteQuery(query, &DeleteParams{
		Cond: "@if(gt(len(schoolId), 0))", // condition on delete query
		Nodes: []DeleteNode{
			{UID: userUID},
		},
	})
	if err != nil {
		panic(err)
	}

	// scan the query result on to the passed query model(s)
	err = result.Scan()
	if err != nil {
		panic(err)
	}

	// should be populated according to the query
	fmt.Println(queryUser.Schools)
```

#### Delete Node

`DeleteNode` is a delete helper to delete node(s) by its uid.

```go
	tx := NewTxn(c).SetCommitNow()
	if err := tx.DeleteNode("0x12", "0xff"); err != nil {
		panic(err)
	}
```

#### Delete Edges

For deleting edges, you only need to specify node UID, edge predicate, and edge UIDs

```go
	tx := dgman.NewTxn(c).SetCommitNow()
	if err := tx.DeleteEdge("0x12", "schools", "0x13", "0x14"); err != nil {
		panic(err)
	}
```

If no edge UIDs are specified, all edges of the specified predicate will be deleted.

```go
	tx := dgman.NewTxn(c).SetCommitNow()
	if err := tx.DeleteEdge("0x12", "schools"); err != nil {
		panic(err)
	}
```

## Development

Make sure you have a running `dgraph` cluster, and set the `DGMAN_TEST_DATABASE` environment variable to the connection string of your `dgraph alpha` grpc connection, e.g: `localhost:9080`.

Run the tests:

```
go test -v .
```
