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
	"log"
	"reflect"
	"strings"

	"github.com/dgraph-io/dgo/protos/api"
	"github.com/kr/logfmt"

	"github.com/dgraph-io/dgo"
)

const tagName = "dgraph"

type rawSchema struct {
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

// SchemaMap maps the underlying schema defined for a predicate
type SchemaMap map[string]*Schema

func (s SchemaMap) String() string {
	schemaDef := ""
	for _, schema := range s {
		schemaDef += schema.String() + "\n"
	}
	return schemaDef
}

func (s SchemaMap) Len() int {
	l := 0
	for range s {
		l++
	}
	return l
}

func marshalSchema(initSchemaMap SchemaMap, models ...interface{}) SchemaMap {
	// schema map maps predicates to its index/schema definition
	// to make sure it is unique
	schemaMap := make(SchemaMap)
	if initSchemaMap != nil {
		schemaMap = initSchemaMap
	}

	for _, model := range models {
		current, err := reflectType(model)
		if err != nil {
			log.Println(err)
			continue
		}

		nodeType := toSnakeCase(current.Name())
		schemaMap[nodeType] = &Schema{
			Predicate: nodeType,
			Type:      "string",
		}

		numFields := current.NumField()
		for i := 0; i < numFields; i++ {
			field := current.Field(i)

			s, err := parseDgraphTag(&field)
			if err != nil {
				log.Println("unmarshal dgraph tag: ", err)
				continue
			}

			schema, _ := schemaMap[s.Predicate]
			// don't parse struct composition fields (empty name), don't need to parse uid
			if s.Predicate != "" && s.Predicate != "uid" {
				// edge
				if s.Type == "uid" {
					// traverse node
					edgePtr := reflect.New(field.Type.Elem())
					marshalSchema(schemaMap, edgePtr.Elem().Interface())
				}

				if schema != nil && schema.String() != s.String() {
					log.Printf("conflicting schema %s, already defined as \"%s\", trying to define \"%s\"\n", s.Predicate, schema.String(), s.String())
				} else {
					schemaMap[s.Predicate] = s
				}
			}
		}
	}
	return schemaMap
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
		if sliceType.Kind() == reflect.Struct {
			// assume is edge
			schemaType = "uid"
		} else {
			schemaType = fmt.Sprintf("[%s]", sliceType.Name())
		}
	case reflect.Struct:
		switch fieldType.PkgPath() {
		case "time":
			schemaType = "dateTime"
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		schemaType = "int"
	case reflect.Float32, reflect.Float64:
		schemaType = "float"
	}

	// check if custom struct/type specifies a scalar type
	// from CustomScalar interface
	ptr := reflect.New(fieldType)
	if scalar, ok := ptr.Elem().Interface().(CustomScalar); ok {
		schemaType = scalar.ScalarType()
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

	tx := c.NewTxn()

	resp, err := tx.Query(context.Background(), schemaQuery)
	if err != nil {
		return nil, err
	}

	schemas := make([]*Schema, len(resp.Schema))
	for index, schema := range resp.Schema {
		// temporary use own schema defition
		// TODO: use dgo builtin *api.SchemaNode
		schemas[index] = &Schema{
			Predicate: schema.Predicate,
			Type:      schema.Type,
			Index:     schema.Index,
			Reverse:   schema.Reverse,
			Tokenizer: schema.Tokenizer,
			List:      schema.List,
			Count:     schema.Count,
			Upsert:    schema.Upsert,
		}
	}

	return schemas, nil
}

// CreateSchema generate indexes and schema from struct models,
// returns the created schemap.
func CreateSchema(c *dgo.Dgraph, models ...interface{}) (*SchemaMap, error) {
	definedSchema := marshalSchema(nil, models...)
	existingSchema, err := fetchExistingSchema(c)
	if err != nil {
		return nil, err
	}

	for _, schema := range existingSchema {
		if s, exists := definedSchema[schema.Predicate]; exists {
			if s.String() != schema.String() {
				log.Printf("existing schema %s, already defined as \"%s\", trying to install \"%s\"\n", schema.Predicate, schema.String(), s.String())
			}

			delete(definedSchema, schema.Predicate)
		}
	}

	if len(definedSchema) > 0 {
		if err = c.Alter(context.Background(), &api.Operation{Schema: definedSchema.String()}); err != nil {
			return nil, err
		}
	}
	return &definedSchema, err
}
