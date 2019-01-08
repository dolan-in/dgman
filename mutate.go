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
	"fmt"
	"log"
	"reflect"

	"github.com/dgraph-io/dgo"
	"github.com/dgraph-io/dgo/protos/api"
)

// UniqueError returns the field and value that failed the unique node check
type UniqueError struct {
	NodeType string
	Field    string
	Value    interface{}
}

func (u UniqueError) Error() string {
	return fmt.Sprintf("%s with %s %v already exists\n", u.NodeType, u.Field, u.Value)
}

type NotNullError struct {
	Field string
}

func (n NotNullError) Error() string {
	return fmt.Sprintf("%s must not be null or zero\n", n.Field)
}

type mutateType struct {
	vType     reflect.Type
	schema    map[int]*Schema // maps struct index to dgraph schema
	predIndex map[string]int  // maps predicate struct index
}

func newMutateType(model interface{}) (*mutateType, error) {
	mType := &mutateType{}
	mType.schema = make(map[int]*Schema)
	mType.predIndex = make(map[string]int)

	vType, err := reflectType(model)
	if err != nil {
		return nil, err
	}

	mType.vType = vType

	numFields := vType.NumField()
	for i := 0; i < numFields; i++ {
		structField := vType.Field(i)

		s, err := parseDgraphTag(&structField)
		if err != nil {
			return nil, err
		}

		mType.schema[i] = s
		mType.predIndex[s.Predicate] = i
	}

	return mType, nil
}

func (m *mutateType) uidIndex() (int, error) {
	index, ok := m.predIndex["uid"]
	if !ok {
		return -1, fmt.Errorf("uid field is not present in struct")
	}
	return index, nil
}

// saveUID saves the UID to the passed model, field with uid json tag
func (m *mutateType) saveUID(uids map[string]string, model interface{}) error {
	val, err := reflectValue(model)
	if err != nil {
		return err
	}

	if len(uids) == 1 {
		// single node, just set the uid
		uidIndex, err := m.uidIndex()
		if err != nil {
			return err
		}

		field := val.Field(uidIndex)
		for _, uid := range uids {
			field.SetString(uid)
			return nil
		}
	} else if len(uids) > 1 {
		// passed model was a slice, so multiple nodes
		// iterate the uid list and set the uid
		uidIndex, err := m.uidIndex()
		if err != nil {
			return err
		}

		n := len(uids)
		for i := 0; i < n; i++ {
			uidAlias := blankUID(i)
			uid := uids[uidAlias]

			el := val.Index(i)

			if el.Kind() == reflect.Ptr {
				el = el.Elem()
			}

			el.Field(uidIndex).SetString(uid)
		}
	}

	return nil
}

// MutateOptions specifies options for mutating
type MutateOptions struct {
	DisableInject bool
	CommitNow     bool
}

func mutate(ctx context.Context, tx *dgo.Txn, data interface{}, opt MutateOptions) (*api.Assigned, error) {
	out, err := marshalAndInjectType(data, opt.DisableInject)
	if err != nil {
		return nil, err
	}

	return tx.Mutate(ctx, &api.Mutation{
		SetJson:   out,
		CommitNow: opt.CommitNow,
	})
}

// Mutate is a shortcut to create mutations from data to be marshalled into JSON,
// it will inject the node type from the Struct name converted to snake_case
func Mutate(ctx context.Context, tx *dgo.Txn, data interface{}, options ...MutateOptions) error {
	opt := MutateOptions{}
	if len(options) > 0 {
		opt = options[0]
	}

	mType, err := newMutateType(data)
	if err != nil {
		return err
	}

	assigned, err := mutate(ctx, tx, data, opt)
	if err != nil {
		return err
	}

	return mType.saveUID(assigned.Uids, data)
}

func mutateWithConstraints(ctx context.Context, tx *dgo.Txn, data interface{}, update bool, options ...MutateOptions) error {
	opt := MutateOptions{}
	if len(options) > 0 {
		opt = options[0]
	}

	mType, err := newMutateType(data)
	if err != nil {
		return err
	}

	if err := mType.constraintChecks(ctx, tx, data, update); err != nil {
		return err
	}

	assigned, err := mutate(ctx, tx, data, opt)
	if err != nil {
		return err
	}

	// if not update save uid
	if !update {
		return mType.saveUID(assigned.Uids, data)
	}

	return nil
}

// Create creates a node with field unique checking
func Create(ctx context.Context, tx *dgo.Txn, data interface{}, options ...MutateOptions) error {
	return mutateWithConstraints(ctx, tx, data, false, options...)
}

// Update updates a node by their UID with field unique checking
func Update(ctx context.Context, tx *dgo.Txn, data interface{}, options ...MutateOptions) error {
	return mutateWithConstraints(ctx, tx, data, true, options...)
}

// blankUID generates alias for blank uid from slice index
func blankUID(index int) string {
	return fmt.Sprintf("node-%d", index)
}

func (m *mutateType) constraintChecks(ctx context.Context, tx *dgo.Txn, data interface{}, update bool) error {
	val, err := reflectValue(data)
	if err != nil {
		return err
	}

	modelType := val.Type()

	switch modelType.Kind() {
	case reflect.Slice:
		len := val.Len()
		for i := 0; i < len; i++ {
			v := val.Index(i)
			if err := m.unique(ctx, tx, v.Interface(), update); err != nil {
				return err
			}

			// if not update, uid should be empty, add formatted alias for easy reference
			if !update {
				uidIndex, err := m.uidIndex()
				if err != nil {
					return err
				}

				if v.Kind() == reflect.Ptr {
					v = v.Elem()
				}

				if v.Field(uidIndex).Interface() == "" {
					v.Field(uidIndex).SetString("_:" + blankUID(i))
				}
			}
		}
	case reflect.Struct:
		if err := m.unique(ctx, tx, data, update); err != nil {
			return err
		}
	}

	return nil
}

func (m *mutateType) unique(ctx context.Context, tx *dgo.Txn, data interface{}, update bool) error {
	uniqueFields, err := m.getAllUniqueFields(data, update)
	if err != nil {
		return err
	}

	if update {
		uidIndex, _ := m.uidIndex()
		uid := uniqueFields[uidIndex].(string)

		node := reflect.New(m.vType).Interface()
		if err := GetByUID(ctx, tx, uid, node); err != nil {
			return err
		}

		// make sure uid not unique checkd
		delete(uniqueFields, uidIndex)

		val, err := reflectValue(node)
		if err != nil {
			return err
		}

		uniqueFieldsCopy := make(map[int]interface{})
		for k, v := range uniqueFields {
			uniqueFieldsCopy[k] = v
		}

		// delete all unmodified fields, to avoid unique checking
		for index := range uniqueFieldsCopy {
			if val.Field(index).Interface() == uniqueFields[index] {
				delete(uniqueFields, index)
			}
		}
	}

	for index, value := range uniqueFields {
		s := m.schema[index]
		if exist, err := exists(ctx, tx, s.Predicate, value, data); exist {
			nodeType := GetNodeType(data)
			return UniqueError{nodeType, s.Predicate, value}
		} else if err != nil {
			return err
		}
	}

	return nil
}

func exists(ctx context.Context, tx *dgo.Txn, field string, value interface{}, model interface{}) (bool, error) {
	jsonValue, err := json.Marshal(value)
	if err != nil {
		return false, err
	}

	filter := fmt.Sprintf(`eq(%s, %s)`, field, jsonValue)
	if err := GetByFilter(ctx, tx, filter, model); err != nil {
		if err == ErrNodeNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// getAllUniqueFields gets all values of the fields that has to be unique
// and also checks for not null constraints
func (m *mutateType) getAllUniqueFields(data interface{}, update bool) (map[int]interface{}, error) {
	v, err := reflectValue(data)
	if err != nil {
		return nil, err
	}

	// map all fields that must be unique
	uniqueValueMap := make(map[int]interface{})
	if update {
		// if update, save the uid also
		uidIndex, err := m.uidIndex()
		if err != nil {
			return nil, err
		}

		uniqueValueMap[uidIndex] = v.Field(uidIndex).Interface()
	}

	for i := 0; i < m.vType.NumField(); i++ {
		field := v.Field(i)
		s := m.schema[i]

		if s.Unique {
			val := field.Interface()
			if isNull(val) {
				// only check not null if not update
				if !update {
					if s.NotNull {
						return nil, NotNullError{s.Predicate}
					}
				}
				// if not null is not set, don't check for unique if value is null
				continue
			}
			uniqueValueMap[i] = val
		}
	}
	return uniqueValueMap, nil
}

func isNull(x interface{}) bool {
	return x == nil || reflect.DeepEqual(x, reflect.Zero(reflect.TypeOf(x)).Interface())
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
	if node, ok := data.(NodeType); ok {
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
			elem := dataType.Elem()

			if elem.Kind() == reflect.Ptr {
				elem = elem.Elem()
			}

			structName = elem.Name()
		}
	}
	return toSnakeCase(structName)
}
