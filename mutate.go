package dgman

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"

	"github.com/dgraph-io/dgo"
	"github.com/dgraph-io/dgo/protos/api"
)

// UniqueError returns the field and value that failed the unique node check
type UniqueError struct {
	Field string
	Value interface{}
}

func (u UniqueError) Error() string {
	return fmt.Sprintf("%s %v already exists\n", u.Field, u.Value)
}

// MutateOptions specifies options for mutating
type MutateOptions struct {
	DisableInject bool
	CommitNow     bool
}

// Mutate is a shortcut to create mutations from data to be marshalled into JSON,
// it will inject the node type from the Struct name converted to snake_case
func Mutate(ctx context.Context, tx *dgo.Txn, data interface{}, options ...MutateOptions) (uids map[string]string, err error) {
	opt := MutateOptions{}
	if len(options) > 0 {
		opt = options[0]
	}

	out, err := marshalAndInjectType(data, opt.DisableInject)
	if err != nil {
		log.Println("marshal", err)
		return nil, err
	}

	assigned, err := tx.Mutate(ctx, &api.Mutation{
		SetJson:   out,
		CommitNow: opt.CommitNow,
	})
	if err != nil {
		log.Println("mutate", err)
		return nil, err
	}

	return assigned.Uids, nil
}

// Create is similar to Mutate, but checks for fields that must be unique for a certain node type
func Create(ctx context.Context, tx *dgo.Txn, model interface{}, opt ...MutateOptions) (uids map[string]string, err error) {
	modelType, err := reflectType(model)
	if err != nil {
		return nil, err
	}

	switch modelType.Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(model)

		for i := 0; i < s.Len(); i++ {
			v := s.Index(i).Interface()
			if err := unique(ctx, tx, v); err != nil {
				return nil, err
			}
		}
	case reflect.Struct:
		if err := unique(ctx, tx, model); err != nil {
			return nil, err
		}
	}

	return Mutate(ctx, tx, model, opt...)
}

func unique(ctx context.Context, tx *dgo.Txn, model interface{}) error {
	uniqueFields := getAllUniqueFields(model)

	for field, value := range uniqueFields {
		if exists(ctx, tx, field, value, model) {
			return UniqueError{field, value}
		}
	}

	return nil
}

func exists(ctx context.Context, tx *dgo.Txn, field string, value interface{}, model interface{}) bool {
	jsonValue, err := json.Marshal(value)
	if err != nil {
		log.Println("unmarshal", err)
		return false
	}

	filter := fmt.Sprintf(`eq(%s, %s)`, field, jsonValue)
	if err := GetByFilter(ctx, tx, filter, model); err != nil {
		if err != ErrNodeNotFound {
			log.Println("check exist", err)
		}
		return false
	}
	return true
}

// getAllUniqueFields gets all values of the fields that has to be unique
func getAllUniqueFields(model interface{}) map[string]interface{} {
	v, err := reflectValue(model)
	if err != nil {
		return nil
	}
	numFields := v.NumField()

	// map all fields that must be unique
	uniqueValueMap := make(map[string]interface{})
	for i := 0; i < numFields; i++ {
		field := v.Field(i)
		structField := v.Type().Field(i)

		s, err := parseDgraphTag(&structField)
		if err != nil {
			return nil
		}

		if s.Unique {
			uniqueValueMap[s.Predicate] = field.Interface()
		}
	}
	return uniqueValueMap
}

func marshalAndInjectType(data interface{}, disableInject bool) ([]byte, error) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Println("marshal", err)
		return nil, err
	}

	if !disableInject {
		nodeType := GetNodeType(data)

		jsonString := jsonData
		switch string(jsonString[0]) {
		case "{": // if JSON object, starts with "{"
			result := fmt.Sprintf(`{"%s":"",%s`, nodeType, string(jsonData[1:]))
			return []byte(result), nil
		case "[": // if JSON array, starts with "[", inject node type one by one
			result := ""
			for _, char := range jsonString {
				if string(char) == "{" {
					result += fmt.Sprintf(`{"%s":"",`, nodeType)
					continue
				}
				result += string(char)
			}
			return []byte(result), nil
		}
	}

	return jsonData, nil
}

// GetNodeType gets node type from NodeType() method of Node interface
// if it doesn't implement it, get it from the struct name and convert to snake case
func GetNodeType(data interface{}) string {
	// check if data implements node interface
	if node, ok := data.(Node); ok {
		return node.NodeType()
	}
	// get node type from struct name and convert to snake case
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
			structName = dataType.Elem().Name()
		}
	}
	return toSnakeCase(structName)
}
