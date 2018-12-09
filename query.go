package dgman

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dgraph-io/dgo"
)

var (
	ErrNodeNotFound = errors.New("node not found")
)

// GetByUID gets a single node by their UID and returns the value to the passed model struct
func GetByUID(ctx context.Context, tx *dgo.Txn, uid string, model interface{}) error {
	query := fmt.Sprintf(`{
		data(func: uid(%s)) {
			expand(_all_)
		}
	}`, uid)

	resp, err := tx.Query(ctx, query)
	if err != nil {
		return err
	}

	return singleResult(resp.Json, model)
}

// GetByFilter gets a single node by using a Dgraph query filter
// and returns the single value to the passed model struct
func GetByFilter(ctx context.Context, tx *dgo.Txn, filter string, model interface{}) error {
	nodeType := getNodeType(model)
	query := fmt.Sprintf(`{
		data(func: has(%s)) @filter(%s) {
			expand(_all_)
		}
	}`, nodeType, filter)

	resp, err := tx.Query(ctx, query)
	if err != nil {
		return err
	}

	return singleResult(resp.Json, model)
}

// Find returns multiple nodes that matches the specified Dgraph query filter,
// the passed model must be a slice
func Find(ctx context.Context, tx *dgo.Txn, filter string, model interface{}) error {
	nodeType := getNodeType(model)
	query := fmt.Sprintf(`{
		data(func: has(%s)) @filter(%s) {
			expand(_all_)
		}
	}`, nodeType, filter)
	resp, err := tx.Query(ctx, query)
	if err != nil {
		return err
	}

	return multipleResult(resp.Json, model)
}

func singleResult(jsonData []byte, model interface{}) error {
	var result struct {
		Data []json.RawMessage
	}

	if err := json.Unmarshal(jsonData, &result); err != nil {
		return err
	}

	if len(result.Data) == 0 {
		return ErrNodeNotFound
	}

	val := result.Data[0]
	if err := json.Unmarshal(val, model); err != nil {
		return err
	}

	return nil
}

func multipleResult(jsonData []byte, model interface{}) error {
	var result struct {
		Data json.RawMessage
	}

	if err := json.Unmarshal(jsonData, &result); err != nil {
		return err
	}

	if err := json.Unmarshal(result.Data, model); err != nil {
		return err
	}

	return nil
}
