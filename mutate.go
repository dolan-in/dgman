package dgman

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/dgraph-io/dgo"
	"github.com/dgraph-io/dgo/protos/api"
)

// MutateOptions specifies options for mutating
type MutateOptions struct {
	DisableInject bool
	CommitNow     bool
}

// Mutate is a shortcut to create mutations from data to be marshalled into JSON,
// it will inject the node type from the Struct name converted to snake_case
func Mutate(ctx context.Context, tx *dgo.Txn, data interface{}, options ...MutateOptions) (string, error) {
	opt := MutateOptions{}
	if len(options) > 0 {
		opt = options[0]
	}

	out, err := marshalAndInjectType(data, opt.DisableInject)
	if err != nil {
		log.Println("marshal", err)
		return "", err
	}

	assigned, err := tx.Mutate(ctx, &api.Mutation{
		SetJson:   out,
		CommitNow: opt.CommitNow,
	})
	if err != nil {
		log.Println("mutate", err)
		return "", err
	}

	// TODO: handle bulk mutations
	uid, ok := assigned.Uids["blank-0"]
	if !ok {
		// if update, no uid's assigned
		return "", nil
	}
	return uid, nil
}

func CreateUnique(ctx context.Context, tx *dgo.Txn, uniqueField string, model interface{}, opt ...MutateOptions) (uid string, err error) {
	// get the value of the unique field
	v := reflect.TypeOf(model)
	numFields := v.NumField()

	var uniqueFieldValue reflect.Value
	for i := 0; i < numFields; i++ {
		field := v.Field(i)

		jsonTags := strings.Split(field.Tag.Get("json"), ",")
		name := jsonTags[0]

		if name == uniqueField {
			uniqueFieldValue = reflect.ValueOf(model).Field(i)
			break
		}
	}

	filter := fmt.Sprintf(`eq(%s, %v)`, uniqueField, uniqueFieldValue)
	if err := GetByFilter(ctx, tx, filter, model); err != nil {
		return "", err
	}

	if model != nil {
		return "", fmt.Errorf("field %s with value %v already exists", uniqueField, uniqueFieldValue)
	}

	return Mutate(ctx, tx, model, opt...)
}

func marshalAndInjectType(data interface{}, disableInject bool) ([]byte, error) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Println("marshal", err)
		return nil, err
	}

	if !disableInject {
		nodeType := getNodeType(data)

		switch jsonData[0] {
		case 123: // if JSON object, starts with "{" (123 in ASCII)
			result := fmt.Sprintf("{\"%s\":\"\",%s", nodeType, string(jsonData[1:]))
			return []byte(result), nil
		}
	}

	return jsonData, nil
}

func getNodeType(data interface{}) string {
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
