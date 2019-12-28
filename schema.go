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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/dgraph-io/dgo/v2/protos/api"
	"github.com/kr/logfmt"

	"github.com/dgraph-io/dgo/v2"
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
	Unique     bool
	Notnull    bool
}

type Schema struct {
	Predicate string
	Type      string
	Index     bool
	Tokenizer []string
	Reverse   bool
	Count     bool
	List      bool
	Upsert    bool
	Unique    bool
	NotNull   bool
}

func (s Schema) String() string {
	schema := fmt.Sprintf("%s: %s ", s.Predicate, s.Type)
	if s.Index {
		schema += fmt.Sprintf("@index(%s) ", strings.Join(s.Tokenizer, ","))
	}
	if s.Upsert {
		schema += "@upsert "
	}
	if s.Count {
		schema += "@count "
	}
	if s.Reverse {
		schema += "@reverse "
	}
	return schema + "."
}

// TypeMap maps a dgraph type with its predicates
type TypeMap map[string]SchemaMap

func (t TypeMap) String() string {
	var buffer bytes.Buffer
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
	var buffer bytes.Buffer
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
	return t.Schema.String() + t.Types.String()
}

func marshalSchema(initSchemaMap SchemaMap, initTypeMap TypeMap, models ...interface{}) *TypeSchema {
	// schema map maps predicates to its index/schema definition
	// to make sure it is unique
	schemaMap := make(SchemaMap)
	if initSchemaMap != nil {
		schemaMap = initSchemaMap
	}
	typeMap := make(TypeMap)
	if initTypeMap != nil {
		typeMap = initTypeMap
	}

	for _, model := range models {
		current, err := reflectType(model)
		if err != nil {
			log.Println(err)
			continue
		}

		nodeType := GetNodeType(model)
		typeMap[nodeType] = make(SchemaMap)

		numFields := current.NumField()
		for i := 0; i < numFields; i++ {
			field := current.Field(i)

			s, err := parseDgraphTag(&field)
			if err != nil {
				log.Println("unmarshal dgraph tag: ", err)
				continue
			}

			schema, _ := schemaMap[s.Predicate]
			// don't parse struct composition fields (empty name), don't need to parse uid, don't parse facets
			parse := s.Predicate != "" && s.Predicate != "uid" && !strings.Contains(s.Predicate, "|")
			if parse {
				// one-to-one and many-to-many edge
				if s.Type == "uid" || s.Type == "[uid]" {
					edgeType := field.Type

					if edgeType.Kind() == reflect.Ptr {
						edgeType = edgeType.Elem()
					}
					// traverse node
					edgePtr := reflect.New(edgeType)
					marshalSchema(schemaMap, typeMap, edgePtr.Interface())
				}

				// each type should uniquely specify a predicate, that's why use a map on predicate
				typeMap[nodeType][s.Predicate] = s
				if schema != nil && schema.String() != s.String() {
					log.Printf("conflicting schema %s, already defined as \"%s\", trying to define \"%s\"\n", s.Predicate, schema.String(), s.String())
				} else {
					schemaMap[s.Predicate] = s
				}
			}
		}
	}
	return &TypeSchema{Schema: schemaMap, Types: typeMap}
}

// TODO: handle go custom types, e.g: type Enum uint
func getSchemaType(fieldType reflect.Type) string {
	if fieldType.Kind() == reflect.Ptr {
		fieldType = fieldType.Elem()
	}
	schemaType := fieldType.Name()

	switch fieldType.Kind() {
	case reflect.Slice:
		sliceType := fieldType.Elem()

		if sliceType.Kind() == reflect.Ptr {
			sliceType = sliceType.Elem()
		}

		schemaType = fmt.Sprintf("[%s]", getSchemaType(sliceType))
	case reflect.Struct:
		switch fieldType.PkgPath() {
		case "time":
			// golang std time
			schemaType = "datetime"
		default:
			// one-to-one relation
			schemaType = "uid"
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		schemaType = "int"
	case reflect.Float32, reflect.Float64:
		schemaType = "float"
	}

	return schemaType
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
		schema.NotNull = dgraphProps.Notnull

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

func reflectValue(model interface{}) (*reflect.Value, error) {
	current := reflect.ValueOf(model)

	if current.Kind() == reflect.Ptr && !current.IsNil() {
		current = current.Elem()
	}

	if current.Kind() != reflect.Struct && current.Kind() != reflect.Slice && current.Kind() != reflect.Interface {
		return nil, fmt.Errorf("model \"%s\" passed for schema is not a struct or slice", current.Type().Name())
	}

	return &current, nil
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

func cleanExistingTypes(c *dgo.Dgraph, typeMap TypeMap) error {
	existingTypes, err := fetchExistingTypes(c, typeMap)
	if err != nil {
		return err
	}

	for _type := range existingTypes {
		if _, exists := typeMap[_type]; exists {
			log.Println("existing type", _type)

			delete(typeMap, _type)
		}
	}

	return nil
}

// CreateSchema generate indexes and schema from struct models,
// returns the created schema map and types, does not update duplicate/conflict predicates.
func CreateSchema(c *dgo.Dgraph, models ...interface{}) (*TypeSchema, error) {
	typeSchema := marshalSchema(nil, nil, models...)

	err := cleanExistingSchema(c, typeSchema.Schema)
	if err != nil {
		return nil, err
	}
	if err = cleanExistingTypes(c, typeSchema.Types); err != nil {
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
// attempt updates for schema and indexes.
func MutateSchema(c *dgo.Dgraph, models ...interface{}) (*TypeSchema, error) {
	typeSchema := marshalSchema(nil, nil, models...)

	alterString := typeSchema.String()
	if alterString != "" {
		if err := c.Alter(context.Background(), &api.Operation{Schema: alterString}); err != nil {
			return nil, err
		}
	}
	return typeSchema, nil
}

// GetNodeType gets node type from NodeType() method of Node interface
// if it doesn't implement it, get it from the struct name
func GetNodeType(data interface{}) string {
	// check if data implements node interface
	if node, ok := data.(NodeType); ok {
		return node.NodeType()
	}
	// get node type from struct name
	structName := ""
	dataType := reflect.TypeOf(data)

	switch dataType.Kind() {
	case reflect.Struct:
		structName = dataType.Name()
	case reflect.Ptr, reflect.Slice:
		dataType = dataType.Elem()
		switch dataType.Kind() {
		case reflect.Struct:
			structName = dataType.Name()
		case reflect.Ptr, reflect.Slice:
			elem := dataType.Elem()

			if elem.Kind() == reflect.Ptr {
				elem = elem.Elem()
			}

			structName = elem.Name()
		}
	}
	return structName
}
