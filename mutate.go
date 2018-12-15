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
func Mutate(ctx context.Context, tx *dgo.Txn, data interface{}, options ...MutateOptions) error {
	opt := MutateOptions{}
	if len(options) > 0 {
		opt = options[0]
	}

	out, err := marshalAndInjectType(data, opt.DisableInject)
	if err != nil {
		return err
	}

	assigned, err := tx.Mutate(ctx, &api.Mutation{
		SetJson:   out,
		CommitNow: opt.CommitNow,
	})
	if err != nil {
		return err
	}

	return saveUID(assigned.Uids, data)
}

// saveUID saves the UID to the passed model, field with uid json tag
func saveUID(uids map[string]string, model interface{}) error {
	v, err := reflectValue(model)
	if err != nil {
		return err
	}

	if len(uids) == 1 {
		numFields := v.NumField()
		// single node, just set the uid
		for i := 0; i < numFields; i++ {
			field := v.Field(i)

			fieldType := v.Type().Field(i)
			name := getPredicate(&fieldType)

			if name == "uid" {
				for _, uid := range uids {
					field.SetString(uid)
					return nil
				}
			}
		}
	} else if len(uids) > 1 {
		// passed model was a slice, so multiple nodes
		// iterate the uid list and set the uid
		sliceType, err := reflectType(v.Interface())
		if err != nil {
			return err
		}

		// iterate the fields, find the uid field index
		numFields := sliceType.NumField()
		uidIndex := 0
		for i := 0; i < numFields; i++ {
			fieldType := sliceType.Field(i)
			name := getPredicate(&fieldType)

			if name == "uid" {
				uidIndex = i
				break
			}
		}

		// set uids
		i := 0
		for _, uid := range uids {
			el := v.Index(i)
			el.Field(uidIndex).SetString(uid)

			i++
		}
	}

	return nil
}

// Create is similar to Mutate, but checks for fields that must be unique for a certain node type
func Create(ctx context.Context, tx *dgo.Txn, model interface{}, opt ...MutateOptions) error {
	modelType := reflect.TypeOf(model)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}

	switch modelType.Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(model)
		if s.Type().Kind() == reflect.Ptr {
			s = s.Elem()
		}

		for i := 0; i < s.Len(); i++ {
			v := s.Index(i).Interface()
			if err := unique(ctx, tx, v); err != nil {
				return err
			}
		}
	case reflect.Struct:
		if err := unique(ctx, tx, model); err != nil {
			return err
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
