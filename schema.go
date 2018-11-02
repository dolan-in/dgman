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
	if s.Index {
		return fmt.Sprintf("%s: %s @index(%s) .", s.Predicate, s.Type, strings.Join(s.Tokenizer, ","))
	}
	return fmt.Sprintf("%s: %s .", s.Predicate, s.Type)
}

type SchemaMap map[string]*Schema

func (s SchemaMap) String() string {
	schemaDef := ""
	for _, schema := range s {
		schemaDef += schema.String() + "\n"
	}
	return schemaDef
}

func marshalSchema(models ...interface{}) SchemaMap {
	// schema map maps predicates to its index/schema definition
	// to make sure it is unique
	schemaMap := make(SchemaMap)
	for _, model := range models {
		current, err := reflectType(model)
		if err != nil {
			log.Println(err)
			continue
		}

		numFields := current.NumField()
		for i := 0; i < numFields; i++ {
			field := current.Field(i)

			jsonTags := strings.Split(field.Tag.Get("json"), ",")
			name := jsonTags[0]
			// check if field already exists
			if schema, ok := schemaMap[name]; !ok {
				// uid may need different parser
				if name != "uid" {
					s := &Schema{
						Predicate: name,
						Type:      field.Type.Name(),
					}

					dgraphTag := field.Tag.Get(tagName)

					if dgraphTag != "" {
						dgraphProps, err := parseStructTag(dgraphTag)
						if err != nil {
							log.Println("unmarshal dgraph tag: ", err)
							continue
						}

						s.Index = dgraphProps.Index != ""
						s.List = dgraphProps.List
						s.Upsert = dgraphProps.Upsert
						s.Count = dgraphProps.Count

						if s.Index {
							s.Tokenizer = strings.Split(dgraphProps.Index, ",")
						}
					}

					schemaMap[name] = s
				}
			} else {
				log.Printf("duplicate schema %s, already defined as \"%s\n\"", name, schema)
			}
		}
	}
	return schemaMap
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
	definedSchema := marshalSchema(models...)
	existingSchema, err := fetchExistingSchema(httpUri)
	if err != nil {
		return nil, err
	}

	var conflicted []*Schema
	for _, schema := range existingSchema {
		if s, exists := definedSchema[schema.Predicate]; exists {
			if s.String() != schema.String() {
				log.Printf("conflicting schema %s, already defined as \"%s\", trying to install \"%s\"\n", schema.Predicate, schema.String(), s.String())
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
