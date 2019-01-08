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
			uid
			expand(_all_)
		}
	}`, uid)

	resp, err := tx.Query(ctx, query)
	if err != nil {
		return err
	}

	return Node(resp.Json, model)
}

// GetByFilter gets a single node by using a Dgraph query filter
// and returns the single value to the passed model struct
func GetByFilter(ctx context.Context, tx *dgo.Txn, filter string, model interface{}) error {
	nodeType := GetNodeType(model)
	query := fmt.Sprintf(`{
		data(func: has(%s)) @filter(%s) {
			uid
			expand(_all_)
		}
	}`, nodeType, filter)

	resp, err := tx.Query(ctx, query)
	if err != nil {
		return err
	}

	return Node(resp.Json, model)
}

// GetByQuery gets a single node using a query
func GetByQuery(ctx context.Context, tx *dgo.Txn, query string, model interface{}) error {
	nodeType := GetNodeType(model)
	q := fmt.Sprintf(`{
		data(func: has(%s)) %s 
	}`, nodeType, query)

	resp, err := tx.Query(ctx, q)
	if err != nil {
		return err
	}

	return Node(resp.Json, model)
}

// Find returns multiple nodes that matches the specified Dgraph query filter,
// the passed model must be a pointer to a slice
func Find(ctx context.Context, tx *dgo.Txn, filter string, model interface{}) error {
	nodeType := GetNodeType(model)
	query := fmt.Sprintf(`{
		data(func: has(%s)) @filter(%s) {
			uid
			expand(_all_)
		}
	}`, nodeType, filter)
	resp, err := tx.Query(ctx, query)
	if err != nil {
		return err
	}

	return Nodes(resp.Json, model)
}

// Node marshals a single node to a single object of model,
// returns error if no nodes are found,
// query root must be data(func ...)
func Node(jsonData []byte, model interface{}) error {
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

// Nodes marshals multiple nodes to a slice of model,
// query root must be data(func ...)
func Nodes(jsonData []byte, model interface{}) error {
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
