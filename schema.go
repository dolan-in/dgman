package dgman

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
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

type SchemaMap map[string]*Schema

func (s SchemaMap) String() string {
	schemaDef := ""
	for _, schema := range s {
		schemaDef += schema.String() + "\n"
	}
	return schemaDef
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

			jsonTags := strings.Split(field.Tag.Get("json"), ",")
			name := jsonTags[0]

			schema, _ := schemaMap[name]
			// uid may need different parser
			if name != "uid" {
				s, err := parseDgraphTag(name, &field)
				if err != nil {
					log.Println("unmarshal dgraph tag: ", err)
					continue
				}

				// edge
				if s.Type == "uid" {
					// traverse node
					edgePtr := reflect.New(field.Type.Elem())
					marshalSchema(schemaMap, edgePtr.Elem().Interface())
				}

				if schema != nil && schema.String() != s.String() {
					log.Printf("conflicting schema %s, already defined as \"%s\", trying to define \"%s\"\n", name, schema.String(), s.String())
				} else {
					schemaMap[name] = s
				}
			}
		}
	}
	return schemaMap
}

func parseDgraphTag(predicate string, field *reflect.StructField) (*Schema, error) {
	schema := &Schema{
		Predicate: predicate,
		Type:      field.Type.Name(),
	}

	if field.Type.Kind() == reflect.Slice {
		sliceType := field.Type.Elem()
		if sliceType.Kind() == reflect.Struct {
			// assume is edge
			schema.Type = "uid"
		} else {
			schema.Type = fmt.Sprintf("[%s]", sliceType.Name())
		}
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

		if schema.Index {
			schema.Tokenizer = strings.Split(dgraphProps.Index, ",")
		}
	}
	return schema, nil
}

func reflectType(model interface{}) (reflect.Type, error) {
	current := reflect.TypeOf(model)

	if current.Kind() == reflect.Ptr && current != nil {
		current = current.Elem()
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

func fetchExistingSchema(httpUri string) ([]Schema, error) {
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

	r := bytes.NewReader([]byte(schemaQuery))
	resp, err := http.Post(fmt.Sprintf("http://%s/query", httpUri), "application/json", r)
	if err != nil {
		return nil, err
	}

	var queryResult struct {
		Data struct {
			Schema []Schema
		}
	}

	if err := json.NewDecoder(resp.Body).Decode(&queryResult); err != nil {
		return nil, err
	}

	return queryResult.Data.Schema, nil
}

// CreateSchema generate indexes and schema from struct models,
// returns conflicted schemas, useful for testing.
// Currently fetching schema with gRPC not working, workaround: use HTTP.
// https://github.com/dgraph-io/dgo/issues/23
func CreateSchema(c *dgo.Dgraph, httpUri string, models ...interface{}) ([]*Schema, error) {
	definedSchema := marshalSchema(nil, models...)
	existingSchema, err := fetchExistingSchema(httpUri)
	if err != nil {
		return nil, err
	}

	var conflicted []*Schema
	for _, schema := range existingSchema {
		if s, exists := definedSchema[schema.Predicate]; exists {
			if s.String() != schema.String() {
				log.Printf("existing schema %s, already defined as \"%s\", trying to install \"%s\"\n", schema.Predicate, schema.String(), s.String())
				conflicted = append(conflicted, s)

				delete(definedSchema, schema.Predicate)
			}
		}
	}

	if err = c.Alter(context.Background(), &api.Operation{Schema: definedSchema.String()}); err != nil {
		return nil, err
	}
	return conflicted, err
}
