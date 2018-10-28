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

// MutateOptions specifies options for mutating
type MutateOptions struct {
	DisableInject bool
}

// Node is an interface for passing node type
type Node interface {
	NodeType() string
}

// Mutate is a shortcut to create mutations from data to be marshalled into JSON
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
		SetJson: out,
	})
	if err != nil {
		log.Println("mutate", err)
		return "", err
	}

	uid, ok := assigned.Uids["blank-0"]
	if !ok {
		// if update, no uid's assigned
		return "", nil
	}
	return uid, nil
}

func marshalAndInjectType(data interface{}, disableInject bool) ([]byte, error) {
	nodeType := getNodeType(data)
	snakeCase := toSnakeCase(nodeType)

	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Println("marshal", err)
		return nil, err
	}

	if !disableInject {
		switch jsonData[0] {
		case 123: // if JSON object, starts with "{" (123 in ASCII)
			result := fmt.Sprintf("{\"%s\":\"\",%s", snakeCase, string(jsonData[1:]))
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
	return reflect.TypeOf(data).Elem().Name()
}
