
[![Build Status](https://travis-ci.com/dolan-in/dgman.svg?branch=master)](https://travis-ci.com/dolan-in/dgman)
[![Coverage Status](https://coveralls.io/repos/github/dolan-in/dgman/badge.svg?branch=master)](https://coveralls.io/github/dolan-in/dgman?branch=master)
[![GoDoc](https://godoc.org/github.com/dolan-in/dgman?status.svg)](https://godoc.org/github.com/dolan-in/dgman)

***Dgman*** is a schema manager for [Dgraph](https://dgraph.io/) using the [Go Dgraph client (dgo)](https://github.com/dgraph-io/dgo), which manages Dgraph schema and indexes from Go tags in struct definitions

## Features
- Create schemas and indexes from struct tags.
- Detect conflicts from existing schema and defined schema.
- Autoinject [node type](https://docs.dgraph.io/howto/#giving-nodes-a-type) from struct.
- Field unique checking (e.g: emails, username).
- Query helpers.

## Usage 

### Schema Definition

Schemas are defined using Go structs which defines the predicate name from the `json` tag, indices and directives using the `dgraph` tag.

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
	Created  time.Time  `json:"created,omitempty" dgraph:"index=year,month,day"` // will be inferred as dateTime schema type, with year, month, and day indices
	Mobiles  []string   `json:"mobiles,omitempty"` // will be inferred as using the  [string] schema type, slices with primitive types will all be inferred as lists
	Schools  []School   `json:"schools,omitempty" dgraph:"count reverse"` // defines an edge to other nodes, add count index, add reverse edges
}

// School is another node, that will be connected to User node using the schools predicate
type School struct {
	UID      string `json:"uid,omitempty"`
	Name     string `json:"name,omitempty"`
	Location GeoLoc `json:"location,omitempty" dgraph:"type=geo"` // for geo schema type, need to specify explicitly
}


// If custom types are used, you need to specity the type in the ScalarType() method
type EnumType int

func (e EnumType) ScalarType() string {
	return "int"
}

type GeoLoc struct {
	Type  string    `json:"type"`
	Coord []float64 `json:"coordinates"`
}

```