/*
 * Copyright (C) 2018-2020 Dolan and Contributors
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
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/dgraph-io/dgo/v200/protos/api"
	"github.com/kr/logfmt"

	"github.com/dgraph-io/dgo/v200"
)

const tagName = "dgraph"

type rawSchema struct {
	Predicate  string
	Index      string
	Constraint string
	Reverse    bool
	Count      bool
	List       bool
	Upsert     bool
	Type       string
	Noconflict bool
	Unique     bool
}

type Schema struct {
	Predicate  string
	Type       string
	Index      bool
	Tokenizer  []string
	Reverse    bool
	Count      bool
	List       bool
	Upsert     bool
	Noconflict bool
	Unique     bool
}

func (s Schema) String() string {
	schema := fmt.Sprintf("%s: %s ", s.Predicate, s.Type)
	if s.Index {
		schema += fmt.Sprintf("@index(%s) ", strings.Join(s.Tokenizer, ","))
	}
	if s.Upsert || s.Unique {
		schema += "@upsert "
	}
	if s.Count {
		schema += "@count "
	}
	if s.Reverse {
		schema += "@reverse "
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
func (t *TypeSchema) Marshal(parseType bool, models ...interface{}) {
	for _, model := range models {
		current, err := reflectType(model)
		if err != nil {
			log.Println(err)
			continue
		}

		nodeType := GetNodeType(model)
		if parseType {
			t.Types[nodeType] = make(SchemaMap)
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
				t.Marshal(false, fieldPtr.Interface())
				continue
			}

			s, err := parseDgraphTag(&field)
			if err != nil {
				log.Println("unmarshal dgraph tag: ", err)
				continue
			}

			schema, exists := t.Schema[s.Predicate]
			parse := s.Predicate != "" &&
				s.Predicate != "uid" && // don't parse uid
				s.Predicate != dgraphTypePredicate && // don't parse dgraph.type
				!strings.Contains(s.Predicate, "|") // don't parse facet
			if parse {
				// one-to-one and many-to-many edge
				if s.Type == "uid" || s.Type == "[uid]" {
					// traverse node
					edgePtr := reflect.New(fieldType)
					t.Marshal(true, edgePtr.Interface())
				}

				// each type should uniquely specify a predicate, that's why use a map on predicate
				if parseType {
					t.Types[nodeType][s.Predicate] = s
				}
				if exists && schema.String() != s.String() {
					log.Printf("conflicting schema %s, already defined as \"%s\", trying to define \"%s\"\n", s.Predicate, schema.String(), s.String())
				} else {
					t.Schema[s.Predicate] = s
				}
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
	case reflect.Slice:
		sliceType := fieldType.Elem()
		return fmt.Sprintf("[%s]", getSchemaType(sliceType))
	case reflect.Struct:
		switch fieldType.PkgPath() {
		case "time":
			// golang std time
			return "datetime"
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

func getPredicate(field *reflect.StructField) string {
	// get field name from json tag
	jsonTags := strings.Split(field.Tag.Get("json"), ",")
	return jsonTags[0]
}

func parseDgraphTag(field *reflect.StructField) (*Schema, error) {
	schema := &Schema{
		Predicate: getPredicate(field),
		Type:      getSchemaType(field.Type),
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
	return schema, nil
}

func reflectType(model interface{}) (reflect.Type, error) {
	current := reflect.TypeOf(model)

	if (current.Kind() == reflect.Ptr || current.Kind() == reflect.Slice) && current != nil {
		current = current.Elem()
		// if pointer to slice
		if current.Kind() == reflect.Slice {
			current = current.Elem()

			if current.Kind() == reflect.Ptr {
				current = current.Elem()
			}
		}
	}

	if current.Kind() != reflect.Struct && current.Kind() != reflect.Interface {
		return nil, fmt.Errorf("model \"%s\" passed for schema is not a struct", current.Name())
	}

	return current, nil
}

func parseStructTag(tag string) (*rawSchema, error) {
	var schema rawSchema
	if err := logfmt.Unmarshal([]byte(tag), &schema); err != nil {
		return nil, err
	}
	return &schema, nil
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
				log.Printf("existing schema %s, already defined as \"%s\", trying to install \"%s\"\n", schema.Predicate, schema.String(), s.String())
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
	typeSchema.Marshal(true, models...)

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
	typeSchema.Marshal(true, models...)

	alterString := typeSchema.String()
	if alterString != "" {
		if err := c.Alter(context.Background(), &api.Operation{Schema: alterString}); err != nil {
			return nil, err
		}
	}
	return typeSchema, nil
}

// GetNodeType gets node type from the struct name, or "dgraph" tag
// in the "dgraph.type" predicate/json tag
func GetNodeType(data interface{}) string {
	// get node type from struct name
	nodeType := ""
	dataType := reflect.TypeOf(data)
	for dataType.Kind() != reflect.Struct {
		dataType = dataType.Elem()
	}

	nodeType = dataType.Name()

	for i := dataType.NumField() - 1; i >= 0; i-- {
		field := dataType.Field(i)
		predicate := getPredicate(&field)

		if predicate == dgraphTypePredicate {
			dgraphTag := field.Tag.Get(tagName)
			if dgraphTag != "" {
				nodeType = dgraphTag
			}
			break
		}
	}
	return nodeType
}
