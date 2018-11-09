package dgman

// Node is an interface for passing node types
type Node interface {
	NodeType() string
}

// CustomScalar is an interface for defining scalar type from custom struct types
type CustomScalar interface {
	ScalarType() string
}
