/*
 * Copyright (C) 2018 Dolan and Contributors
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
	"context"
	"fmt"
	"math/big"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/dgraph-io/dgo/v250"
	"github.com/dgraph-io/dgo/v250/protos/api"
)

const (
	tagName             = "dgraph"
	predicateDgraphType = "dgraph.type"
	predicateUid        = "uid"
)

type rawSchema struct {
	Predicate  string
	Index      string
	Constraint string
	Reverse    bool
	Count      bool
	List       bool
	Upsert     bool
	Lang       bool
	Type       string
	Noconflict bool
	Unique     bool
}

type Schema struct {
	Predicate        string
	Type             string
	Index            bool
	Tokenizer        []string
	Reverse          bool
	Count            bool
	List             bool
	Upsert           bool
	Lang             bool
	Noconflict       bool `json:"no_conflict"`
	Unique           bool
	OmitEmpty        bool
	Metric           string
	ManagedReverse   bool   // true if this is a managed reverse edge (json:"~predicate" + dgraph:"reverse")
	ForwardPredicate string // the forward predicate name (without ~) for managed reverse edges
}

func (s Schema) String() string {
	t := s.Type
	if s.List {
		t = fmt.Sprintf("[%s]", t)
	}
	schema := fmt.Sprintf("%s: %s ", s.Predicate, t)
	if s.Unique {
		// Check if hash or exact already exists in tokenizers
		hasHashOrExact := false
		hasHash := false

		for _, tokenizer := range s.Tokenizer {
			if tokenizer == "hash" {
				hasHash = true
				hasHashOrExact = true
			} else if tokenizer == "exact" {
				hasHashOrExact = true
			}
		}

		// Only add hash if neither hash nor exact exists and hash isn't already present
		if !hasHashOrExact && !hasHash {
			s.Tokenizer = append(s.Tokenizer, "hash")
		}
	}
	if s.Index {
		schema += fmt.Sprintf("@index(%s) ", strings.Join(s.Tokenizer, ","))
	} else if len(s.Tokenizer) > 0 {
		// Dgraph does not report an index on float32vector when hnsw is used
		tokenizer := s.Tokenizer[0]
		// Remove quotes from pseudo metric and exponent parameters
		tokenizer = strings.ReplaceAll(tokenizer, "\"metric\":", "metric:")
		tokenizer = strings.ReplaceAll(tokenizer, "\"exponent\":", "exponent:")
		schema += fmt.Sprintf("@index(%s) ", tokenizer)
	}
	if s.Upsert || s.Unique {
		schema += "@upsert "
		switch t {
		case "int", "string":
			schema += "@unique "
		}
	}
	if s.Count {
		schema += "@count "
	}
	if s.Reverse {
		schema += "@reverse "
	}
	if s.Lang {
		schema += "@lang "
	}
	if s.Noconflict {
		schema += "@noconflict "
	}
	return schema + "."
}

// TypeMap maps a dgraph type with its predicates
type TypeMap map[string]SchemaMap

func (t TypeMap) String() string {
	var buffer strings.Builder
	for nodeType, predicates := range t {
		buffer.WriteString("type ")
		buffer.WriteString(nodeType)
		buffer.WriteString(" {\n")
		for predicate := range predicates {
			buffer.WriteString("\t")
			buffer.WriteString(predicate)
			buffer.WriteString("\n")
		}
		buffer.WriteString("}\n")
	}
	return buffer.String()
}

// SchemaMap maps the underlying schema defined for a predicate
type SchemaMap map[string]*Schema

func (s SchemaMap) String() string {
	var buffer strings.Builder
	for _, schema := range s {
		buffer.WriteString(schema.String())
		buffer.WriteString("\n")
	}
	return buffer.String()
}

type TypeSchema struct {
	Types  TypeMap
	Schema SchemaMap
}

func (t *TypeSchema) String() string {
	return strings.Join([]string{t.Schema.String(), t.Types.String()}, "\n")
}

// Marshal marshals passed models into type and schema definitions
func (t *TypeSchema) Marshal(parentType string, models ...interface{}) {
	for _, model := range models {
		current, err := reflectType(model)
		if err != nil {
			Logger().WithName("dgman").Error(err, "reflectType error")
			continue
		}

		if current.Kind() == reflect.Interface {
			// don't parse raw interfaces or it will panic
			continue
		}

		nodeType := GetNodeType(model)
		if _, ok := t.Types[nodeType]; ok {
			continue
		}
		if parentType == "" {
			t.Types[nodeType] = make(SchemaMap)
		} else {
			// allow anonymous fields to be parsed into parent type
			nodeType = parentType
		}

		numFields := current.NumField()
		for i := 0; i < numFields; i++ {
			field := current.Field(i)

			fieldType := field.Type
			if fieldType.Kind() == reflect.Ptr {
				fieldType = fieldType.Elem()
			}

			if fieldType.Kind() == reflect.Struct && field.Anonymous {
				fieldPtr := reflect.New(fieldType)
				t.Marshal(nodeType, fieldPtr.Interface())
				continue
			}

			s, err := parseDgraphTag(&field)
			if err != nil {
				Logger().WithName("dgman").Error(err, "unmarshal dgraph tag")
				continue
			}

			schema, exists := t.Schema[s.Predicate]

			// For managed reverse edges, we still need to traverse the edge types
			// to create their schemas, but we don't add the reverse predicate itself
			if s.ManagedReverse {
				if s.Type == "uid" || s.Type == "[uid]" {
					edgePtr := reflect.New(fieldType)
					t.Marshal("", edgePtr.Interface())
				}
				continue
			}

			parse := s.Predicate != "" &&
				s.Predicate != "uid" && // don't parse uid
				s.Predicate != predicateDgraphType && // don't parse dgraph.type
				!strings.Contains(s.Predicate, "|") && // don't parse facet
				s.Predicate[0] != '~' && // don't parse reverse edge (non-managed)
				!strings.Contains(s.Predicate, "@") // don't parse non-primary lang predicate

			if !parse {
				continue
			}

			// one-to-one and many-to-many edge
			if s.Type == "uid" || s.Type == "[uid]" {
				// traverse node
				edgePtr := reflect.New(fieldType)
				t.Marshal("", edgePtr.Interface())
			}

			// each type should uniquely specify a predicate, that's why use a map on predicate
			t.Types[nodeType][s.Predicate] = s
			if exists && schema.String() != s.String() {
				Logger().WithName("dgman").Info("schema conflict during marshalling", "predicate", s.Predicate, "existing", schema.String(), "new", s.String())
			} else {
				t.Schema[s.Predicate] = s
			}
		}
	}
}

// NewTypeSchema returns a new TypeSchema with allocated Schema and Types
func NewTypeSchema() *TypeSchema {
	return &TypeSchema{
		Schema: make(SchemaMap),
		Types:  make(TypeMap),
	}
}

// TODO: handle go custom types, e.g: type Enum uint
func getSchemaType(fieldType reflect.Type) string {
	if fieldType.Kind() == reflect.Ptr {
		fieldType = fieldType.Elem()
	}

	// check if implements SchemaType
	schemaTypeElem := reflect.New(fieldType).Interface()
	if schemaTyper, ok := schemaTypeElem.(SchemaType); ok {
		return schemaTyper.SchemaType()
	}

	switch fieldType.Kind() {
	case reflect.Interface:
		return "uid"
	case reflect.Slice:
		sliceType := fieldType.Elem()
		return fmt.Sprintf("[%s]", getSchemaType(sliceType))
	case reflect.Struct:
		switch fieldType {
		case reflect.TypeOf(time.Time{}):
			return "datetime"
		case reflect.TypeOf(big.Float{}):
			return "bigfloat"
		case reflect.TypeOf(VectorFloat32{}):
			return "float32vector"
		default:
			// one-to-one relation
			return "uid"
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "int"
	case reflect.Float32, reflect.Float64:
		return "float"
	}

	return fieldType.Name()
}

func getPredicate(field *reflect.StructField) (string, bool) {
	// get field name from json tag
	jsonTags := strings.Split(field.Tag.Get("json"), ",")
	if len(jsonTags) == 2 {
		return jsonTags[0], jsonTags[1] == "omitempty"
	}
	return jsonTags[0], false
}

func parseDgraphTag(field *reflect.StructField) (*Schema, error) {
	predicate, omitEmpty := getPredicate(field)
	schema := &Schema{
		Predicate: predicate,
		Type:      getSchemaType(field.Type),
		OmitEmpty: omitEmpty,
	}

	dgraphTag := field.Tag.Get(tagName)

	if dgraphTag != "" {
		dgraphProps, err := parseStructTag(dgraphTag)
		if err != nil {
			return nil, err
		}

		schema.Index = dgraphProps.Index != ""
		schema.List = dgraphProps.List
		schema.Upsert = dgraphProps.Upsert
		schema.Count = dgraphProps.Count
		schema.Reverse = dgraphProps.Reverse
		schema.Unique = dgraphProps.Unique
		schema.Noconflict = dgraphProps.Noconflict
		schema.Lang = dgraphProps.Lang

		if dgraphProps.Predicate != "" {
			schema.Predicate = dgraphProps.Predicate
		}

		if dgraphProps.Type != "" {
			schema.Type = dgraphProps.Type
		}

		if schema.Index {
			schema.Tokenizer = strings.Split(dgraphProps.Index, ",")
		}
	}

	// Detect managed reverse edge: json:"~predicate" + dgraph:"reverse"
	// This allows defining reverse edges on the "parent" side that will
	// create forward edges on children during mutation
	if strings.HasPrefix(schema.Predicate, "~") && schema.Reverse {
		schema.ManagedReverse = true
		schema.ForwardPredicate = schema.Predicate[1:] // strip the ~
	}

	return schema, nil
}

func getElemType(elemable reflect.Type) reflect.Type {
	if elemable.Kind() == reflect.Slice || elemable.Kind() == reflect.Ptr {
		return getElemType(elemable.Elem())
	}
	return elemable
}

func reflectType(model interface{}) (reflect.Type, error) {
	current := reflect.TypeOf(model)

	current = getElemType(current)

	if current.Kind() != reflect.Struct && current.Kind() != reflect.Interface {
		return nil, fmt.Errorf("model \"%s\" passed for schema is not a struct", current.Name())
	}

	return current, nil
}

func parseStructTag(tag string) (*rawSchema, error) {
	schema := &rawSchema{}

	// Special case for HNSW index - extract it first to prevent splitting issues
	hnswRegex := regexp.MustCompile(`index=hnsw\([^)]+\)`)
	hnswMatch := hnswRegex.FindString(tag)
	if hnswMatch != "" {
		// Extract just the index value (everything after "index=")
		schema.Index = strings.TrimPrefix(hnswMatch, "index=")
		// Remove the HNSW part from the tag to avoid double processing
		tag = strings.Replace(tag, hnswMatch, "", 1)
	}

	// Split by space, but keep quoted substrings together
	fields := regexp.MustCompile(`([\w]+=[^\s"']+|[\w]+\s*=\s*"[^"]*"|[\w]+\s*=\s*'[^']*'|[\w]+|[\w]+=[^\s]+)`).FindAllString(tag, -1)

	for _, field := range fields {
		if field == "" {
			continue
		}
		kv := strings.SplitN(field, "=", 2)
		key := strings.TrimSpace(kv[0])
		var value string
		if len(kv) == 2 {
			value = strings.Trim(strings.TrimSpace(kv[1]), `"'`)
		}

		switch key {
		case "index":
			// Only set the index if we didn't already find an HNSW index
			if schema.Index == "" {
				schema.Index = value
			}
		case "reverse":
			schema.Reverse = true
		case "count":
			schema.Count = true
		case "list":
			schema.List = true
		case "upsert":
			schema.Upsert = true
			schema.Unique = true
		case "lang":
			schema.Lang = true
		case "noconflict":
			schema.Noconflict = true
		case "unique":
			schema.Unique = true
		case "predicate":
			schema.Predicate = value
		case "type":
			schema.Type = value
		}
	}

	return schema, nil
}

func GetSchema(c *dgo.Dgraph) (string, error) {
	schemaQuery := `schema {}`

	tx := c.NewReadOnlyTxn()

	resp, err := tx.Query(context.Background(), schemaQuery)
	if err != nil {
		return "", err
	}
	type schemaResponse struct {
		Schema []*Schema `json:"schema"`
		Types  []struct {
			Fields []struct {
				Name string `json:"name"`
			} `json:"fields"`
			Name string `json:"name"`
		} `json:"types"`
	}
	var schemas schemaResponse
	if err = json.Unmarshal(resp.Json, &schemas); err != nil {
		return "", err
	}

	var sb strings.Builder
	for _, s := range schemas.Schema {
		sb.WriteString(s.String())
		sb.WriteString("\n")
	}
	for _, t := range schemas.Types {
		sb.WriteString("type " + t.Name + " {\n")
		for _, field := range t.Fields {
			sb.WriteString("\t" + field.Name + "\n")
		}
		sb.WriteString("}\n")
	}
	return sb.String(), nil
}

func fetchExistingSchema(c *dgo.Dgraph) ([]*Schema, error) {
	schemaQuery := `
		schema {
			type
			index
			reverse
			tokenizer
			list
			count
			upsert
			lang
			noconflict
		}
	`

	tx := c.NewReadOnlyTxn()

	resp, err := tx.Query(context.Background(), schemaQuery)
	if err != nil {
		return nil, err
	}

	type schemaResponse struct {
		Schema []*Schema `json:"schema"`
	}
	var schemas schemaResponse
	if err = json.Unmarshal(resp.Json, &schemas); err != nil {
		return nil, err
	}

	return schemas.Schema, nil
}

type typeQueryResponse struct {
	Types []struct {
		Fields []struct {
			Name string `json:"name"`
		} `json:"fields"`
		Name string `json:"name"`
	} `json:"types"`
}

func fetchExistingTypes(c *dgo.Dgraph, typeMap TypeMap) (TypeMap, error) {
	// get keys of typeMap
	keys := make([]string, 0, len(typeMap))
	for key := range typeMap {
		keys = append(keys, key)
	}

	typeQuery := "schema(type: [" + strings.Join(keys, ", ") + "]) {}"

	tx := c.NewReadOnlyTxn()

	resp, err := tx.Query(context.Background(), typeQuery)
	if err != nil {
		return nil, err
	}

	var typesResponse typeQueryResponse
	if err = json.Unmarshal(resp.Json, &typesResponse); err != nil {
		return nil, err
	}

	types := make(TypeMap)
	for _, _type := range typesResponse.Types {
		types[_type.Name] = make(SchemaMap)
		for _, field := range _type.Fields {
			types[_type.Name][field.Name] = &Schema{}
		}
	}

	return types, nil
}

func cleanExistingSchema(c *dgo.Dgraph, schemaMap SchemaMap) error {
	existingSchema, err := fetchExistingSchema(c)
	if err != nil {
		return err
	}

	for _, schema := range existingSchema {
		if s, exists := schemaMap[schema.Predicate]; exists {
			if s.String() != schema.String() {
				Logger().WithName("dgman").Info("schema conflict", "predicate", schema.Predicate, "existing", schema.String(), "new", s.String())
			}

			delete(schemaMap, schema.Predicate)
		}
	}

	return nil
}

// CreateSchema generate indexes, schema, and types from struct models,
// returns the created schema map and types, does not update duplicate/conflict predicates.
func CreateSchema(c *dgo.Dgraph, models ...interface{}) (*TypeSchema, error) {
	typeSchema := NewTypeSchema()
	typeSchema.Marshal("", models...)

	// Check for DType field in each model
	for _, model := range models {
		modelType := reflect.TypeOf(model)
		if modelType.Kind() == reflect.Ptr {
			modelType = modelType.Elem()
		}
		foundDType := false
		for i := 0; i < modelType.NumField(); i++ {
			field := modelType.Field(i)
			jsonTag := field.Tag.Get("json")

			// Properly parse the JSON tag to extract just the field name
			tagParts := strings.Split(jsonTag, ",")
			fieldName := strings.TrimSpace(tagParts[0])

			if fieldName == "dgraph.type" {
				foundDType = true
				break
			}
		}
		if !foundDType {
			return nil, fmt.Errorf("missing required field DType []string `json:\"dgraph.type\"` in type %s", modelType.Name())
		}
	}

	err := cleanExistingSchema(c, typeSchema.Schema)
	if err != nil {
		return nil, err
	}

	alterString := typeSchema.String()
	if alterString != "" {
		if err = c.Alter(context.Background(), &api.Operation{Schema: alterString}); err != nil {
			return nil, err
		}
	}
	return typeSchema, nil
}

// MutateSchema generate indexes and schema from struct models,
// attempt updates for type, schema, and indexes.
func MutateSchema(c *dgo.Dgraph, models ...interface{}) (*TypeSchema, error) {
	typeSchema := NewTypeSchema()
	typeSchema.Marshal("", models...)

	alterString := typeSchema.String()
	if alterString != "" {
		if err := c.Alter(context.Background(), &api.Operation{Schema: alterString}); err != nil {
			return nil, err
		}
	}
	return typeSchema, nil
}

func getNodeType(dataType reflect.Type) string {
	// get node type from struct name
	nodeType := ""
	dataType = getElemType(dataType)

	nodeType = dataType.Name()

	for i := dataType.NumField() - 1; i >= 0; i-- {
		field := dataType.Field(i)
		predicate, _ := getPredicate(&field)

		if predicate == predicateDgraphType {
			dgraphTag := field.Tag.Get(tagName)
			if dgraphTag != "" {
				nodeType = dgraphTag
			}
			break
		}
	}
	return nodeType
}

// GetNodeType gets node type from the struct name, or "dgraph" tag
// in the "dgraph.type" predicate/json tag
func GetNodeType(data interface{}) string {
	return getNodeType(reflect.TypeOf(data))
}
