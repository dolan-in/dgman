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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/dgraph-io/dgo/v2"
	"github.com/dgraph-io/dgo/v2/protos/api"
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

type mutateType struct {
	vType     reflect.Type
	value     *reflect.Value
	schema    map[int]*Schema // maps struct index to dgraph schema
	predIndex map[string]int  // maps predicate struct index
}

func reflectValue(model interface{}) (*reflect.Value, error) {
	current := reflect.ValueOf(model)

	if current.Kind() == reflect.Ptr && !current.IsNil() {
		current = current.Elem()
	}

	if current.Kind() != reflect.Struct && current.Kind() != reflect.Slice && current.Kind() != reflect.Interface {
		return nil, fmt.Errorf("model \"%s\" passed for schema is not a struct or slice", current.Type().Name())
	}

	// just use a slice, for unifying handling types
	// slice with 1 length, the struct value as the first element
	if current.Kind() == reflect.Struct {
		slice := reflect.MakeSlice(reflect.SliceOf(reflect.PtrTo(current.Type())), 1, 1)
		slice.Index(0).Set(current.Addr())
		current = slice
	}

	return &current, nil
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

	mType.value, err = reflectValue(model)
	if err != nil {
		return nil, err
	}

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

	if err = mType.injectAlias(); err != nil {
		return nil, err
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
func (m *mutateType) saveUID(uids map[string]string, refVal ...*reflect.Value) error {
	val := m.value
	if len(refVal) != 0 {
		val = refVal[0]
	}

	uidIndex, err := m.uidIndex()
	if err != nil {
		return err
	}

	// reflected value must be a slice
	n := val.Len()
	for i := 0; i < n; i++ {
		el := val.Index(i)
		if el.Kind() == reflect.Ptr {
			el = el.Elem()
		}

		uidAlias := blankUID(i)
		uid, exists := uids[uidAlias]
		if exists {
			el.Field(uidIndex).SetString(uid)
		} else {
			el.Field(uidIndex).SetString("") // don't return node alias if uid not assigned
		}
	}

	return nil
}

// MutateOptions specifies options for mutating
type MutateOptions struct {
	// DisableInject disables injecting node types in "dgraph.type" predicate
	DisableInject bool
	CommitNow     bool
}

func mutate(ctx context.Context, tx *dgo.Txn, data interface{}, opt *MutateOptions) (*api.Response, error) {
	out, err := marshalAndInjectType(data, opt.DisableInject)
	if err != nil {
		return nil, err
	}

	return tx.Mutate(ctx, &api.Mutation{
		SetJson:   out,
		CommitNow: opt.CommitNow,
	})
}

func mutateWithConstraints(ctx context.Context, tx *dgo.Txn, data interface{}, update bool, options ...*MutateOptions) error {
	opt := &MutateOptions{}
	if len(options) > 0 {
		opt = options[0]
	}

	mType, err := newMutateType(data)
	if err != nil {
		return err
	}

	req, err := mType.generateRequest(data, update, opt)
	if err != nil {
		return err
	}

	assigned, err := tx.Do(ctx, req)
	if err != nil {
		return err
	}
	log.Println(assigned)

	// if not update save uid
	if !update {
		return mType.saveUID(assigned.Uids)
	}

	return nil
}

// blankUID generates alias for blank uid from slice index
func blankUID(index int) string {
	return fmt.Sprintf("node-%d", index)
}

// injectAlias injects a node uid alias, for easier referencing nodes on mutate
func (m *mutateType) injectAlias() error {
	uidIndex, err := m.uidIndex()
	if err != nil {
		return err
	}

	n := m.value.Len()
	for i := 0; i < n; i++ {
		el := m.value.Index(i)
		if el.Kind() == reflect.Ptr {
			el = el.Elem()
		}

		if el.Field(uidIndex).Interface() == "" {
			el.Field(uidIndex).SetString("_:" + blankUID(i))
		}
	}

	return nil
}

func (m *mutateType) generateQueryConditions(data interface{}, index int, update bool) (query string, condition string, err error) {
	nodeType := GetNodeType(data)
	uniqueFields, err := m.getAllUniqueFields(data, update)
	if err != nil {
		return "", "", err
	}

	uidIndex, err := m.uidIndex()
	if err != nil {
		return "", "", err
	}

	queries := make([]string, 0, len(uniqueFields))
	conditions := make([]string, 0, len(uniqueFields))
	for schemaIndex, value := range uniqueFields {
		if schemaIndex == uidIndex {
			// don't need to filter uid in query
			continue
		}

		jsonValue, err := json.Marshal(value)
		if err != nil {
			return "", "", err
		}

		schema := m.schema[schemaIndex]
		// index refers to the slice index of data
		queryIndex := fmt.Sprintf("q_%d_%d", index, schemaIndex)
		queries = append(queries, fmt.Sprintf("\t%s as var(func: type(%s)) @filter(eq(%s, %s))", queryIndex, nodeType, schema.Predicate, jsonValue))
		conditions = append(conditions, fmt.Sprintf("eq(len(%s), 0)", queryIndex))
	}

	conditionString := strings.Join(conditions, " AND ")
	if conditionString != "" {
		if update {
			conditionString = fmt.Sprintf("@if(uid(%s) OR (%s))", uniqueFields[uidIndex], conditionString)
		} else {
			conditionString = fmt.Sprintf("@if(%s)", conditionString)
		}
	}

	return strings.Join(queries, "\n"), conditionString, nil
}

// TODO: return UniqueError when a node already exist based on unique fields
func (m *mutateType) generateRequest(data interface{}, update bool, opt *MutateOptions) (req *api.Request, err error) {
	// reflected value must be a slice
	len := m.value.Len()
	queries := make([]string, len)
	mutations := make([]*api.Mutation, len)
	for i := 0; i < len; i++ {
		v := m.value.Index(i)

		query, condition, err := m.generateQueryConditions(v.Interface(), i, update)
		if err != nil {
			return nil, err
		}

		setJSON, err := marshalAndInjectType(v.Interface(), opt.DisableInject)
		if err != nil {
			return nil, err
		}

		queries[i] = query
		mutations[i] = &api.Mutation{
			Cond:    condition,
			SetJson: setJSON,
		}
	}

	return &api.Request{
		Query:     fmt.Sprintf("{\n%s\n}", strings.Join(queries, "\n")),
		Mutations: mutations,
		CommitNow: opt.CommitNow,
	}, nil
}

// getAllUniqueFields gets all values of the fields that has to be unique
// and also checks for not null constraints
func (m *mutateType) getAllUniqueFields(data interface{}, update bool) (map[int]interface{}, error) {
	reflectVal := reflect.ValueOf(data)
	if reflectVal.Kind() == reflect.Ptr {
		reflectVal = reflectVal.Elem()
	}

	// map all fields that must be unique
	uniqueValueMap := make(map[int]interface{})
	if update {
		// if update, save the uid also
		uidIndex, err := m.uidIndex()
		if err != nil {
			return nil, err
		}

		uniqueValueMap[uidIndex] = reflectVal.Field(uidIndex).Interface()
	}

	uniqueType := reflect.TypeOf((*NodeUnique)(nil)).Elem()
	if m.vType.Implements(uniqueType) {
		nodeUnique := reflectVal.Interface().(NodeUnique)

		for _, pred := range nodeUnique.UniqueKeys() {
			if predIndex, ok := m.predIndex[pred]; ok {
				val := reflectVal.Field(predIndex).Interface()
				if !isNull(val) {
					uniqueValueMap[predIndex] = val
				}
			}
		}
	} else {
		for i := 0; i < m.vType.NumField(); i++ {
			field := reflectVal.Field(i)
			s := m.schema[i]

			if s.Unique {
				val := field.Interface()
				if !isNull(val) {
					// only check unique if not null/zero value
					uniqueValueMap[i] = val
				}
			}
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
		return nil, err
	}

	if !disableInject {
		nodeType := GetNodeType(data)

		jsonString := jsonData
		switch jsonString[0] {
		case '{': // if JSON object, starts with "{"
			result := `{"dgraph.type":"` + nodeType + `",` + string(jsonData[1:])
			return []byte(result), nil
		case '[': // if JSON array, starts with "[", inject node type one by one
			var result bytes.Buffer
			for _, char := range jsonString {
				if char == '{' {
					result.WriteString(`{"dgraph.type":"`)
					result.WriteString(nodeType)
					result.WriteString(`",`)
					continue
				}
				result.WriteByte(char)
			}
			return result.Bytes(), nil
		}
	}

	return jsonData, nil
}
