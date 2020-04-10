/*
 * Copyright (C) 2018-2020 Dolan and Contributors
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
	"reflect"
	"strconv"
	"strings"

	"github.com/dgraph-io/dgo/v200"
	"github.com/dgraph-io/dgo/v200/protos/api"
	"github.com/pkg/errors"
)

const (
	dgraphTypePredicate = "dgraph.type"
)

// UniqueError returns the field and value that failed the unique node check
type UniqueError struct {
	NodeType string
	Field    string
	Value    interface{}
	UID      string
}

func (u *UniqueError) Error() string {
	return fmt.Sprintf("%s with %s=%v already exists at uid=%s", u.NodeType, u.Field, u.Value, u.UID)
}

type mutateType struct {
	vType     reflect.Type
	value     *reflect.Value
	schema    map[int]*Schema // maps struct index to dgraph schema
	predIndex map[string]int  // maps predicate struct index
	nodeType  string
}

type node struct {
	UID string `json:"uid"`
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
	mType.nodeType = GetNodeType(model)

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

func mutate(ctx context.Context, tx *dgo.Txn, data interface{}, commitNow ...bool) (*api.Response, error) {
	optCommitNow := false
	if len(commitNow) > 0 {
		optCommitNow = commitNow[0]
	}

	out, err := marshalAndInjectType(data)
	if err != nil {
		return nil, err
	}

	return tx.Mutate(ctx, &api.Mutation{
		SetJson:   out,
		CommitNow: optCommitNow,
	})
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

type mutation struct {
	txn         *dgo.Txn
	ctx         context.Context
	update      bool
	predicate   string
	returnQuery bool
	mType       *mutateType
	commitNow   bool
}

func newMutation(tx *TxnContext, data interface{}, commitNow ...bool) (*mutation, error) {
	optCommitNow := false
	if len(commitNow) > 0 {
		optCommitNow = commitNow[0]
	}

	mType, err := newMutateType(data)
	if err != nil {
		return nil, err
	}

	return &mutation{txn: tx.txn, ctx: tx.ctx, mType: mType, commitNow: optCommitNow}, nil
}

func (m *mutation) do() error {
	req, err := m.generateRequest()
	if err != nil {
		return err
	}

	assigned, err := m.txn.Do(m.ctx, req)
	if err != nil {
		return err
	}

	if assigned.Json != nil {
		if err = m.processQuery(assigned); err != nil {
			return err
		}
	}

	// if not update save uid
	if !m.update {
		return m.saveUID(assigned.Uids)
	}

	return nil
}

func (m *mutation) generateRequest() (req *api.Request, err error) {
	// reflected value must be a slice
	len := m.mType.value.Len()
	queries := make([]string, 0, len)
	mutations := make([]*api.Mutation, len)
	for i := 0; i < len; i++ {
		v := m.mType.value.Index(i)

		query, condition, err := m.generateQueryConditions(v.Interface(), i)
		if err != nil {
			return nil, err
		}

		if v.Kind() != reflect.Ptr {
			v = v.Addr()
		}

		setJSON, err := marshalAndInjectType(v.Interface())
		if err != nil {
			return nil, err
		}

		queries = append(queries, query...)
		mutations[i] = &api.Mutation{
			Cond:    condition,
			SetJson: setJSON,
		}
	}

	queryString := strings.Join(queries, "\n")
	if queryString != "" {
		queryString = fmt.Sprintf("{\n%s\n}", queryString)
	}

	return &api.Request{
		Query:     queryString,
		Mutations: mutations,
		CommitNow: m.commitNow,
	}, nil
}

func (m *mutation) generateQueryConditions(data interface{}, index int) (queries []string, condition string, err error) {
	reflectVal := reflect.ValueOf(data)
	if reflectVal.Kind() == reflect.Ptr {
		reflectVal = reflectVal.Elem()
	}

	uidIndex, err := m.mType.uidIndex()
	if err != nil {
		return nil, "", err
	}

	numField := m.mType.vType.NumField()
	var conditions []string
	for schemaIndex := 0; schemaIndex < numField; schemaIndex++ {
		field := reflectVal.Field(schemaIndex)
		schema := m.mType.schema[schemaIndex]
		// index refers to the slice index of data
		queryIndex := fmt.Sprintf("q_%d_%d", index, schemaIndex)
		uidListIndex := fmt.Sprintf("u_%d_%d", index, schemaIndex)

		if schema.Unique {
			value := field.Interface()
			if isNull(value) {
				// only check unique if not null/zero value
				continue
			}

			jsonValue, err := json.Marshal(value)
			if err != nil {
				return nil, "", errors.Wrapf(err, "marshal %v", value)
			}

			filter := fmt.Sprintf("eq(%s, %s)", schema.Predicate, jsonValue)
			if m.update {
				uid := reflectVal.Field(uidIndex).String()
				// if update make sure not unique checking the current node
				filter = fmt.Sprintf("NOT uid(%s) AND %s", uid, filter)
			}

			queryFields := fmt.Sprintf("%s as uid", uidListIndex)
			if m.returnQuery && (m.predicate == "" || m.predicate == schema.Predicate) {
				queryFields = fmt.Sprintf("%s\nexpand(_all_)", queryFields)
			}
			queries = append(queries, fmt.Sprintf("\t%s(func: type(%s), first: 1) @filter(%s) {\n%s\n}", queryIndex, m.mType.nodeType, filter, queryFields))
			// on upsert field, allow mutation to continue, skip condition
			if m.predicate != schema.Predicate {
				conditions = append(conditions, fmt.Sprintf("eq(len(%s), 0)", uidListIndex))
			}
		}

		if m.predicate != "" && m.predicate == schema.Predicate {
			uidFunc := fmt.Sprintf("uid(%s)", uidListIndex)
			reflectVal.Field(uidIndex).SetString(uidFunc)
		}
	}

	conditionString := strings.Join(conditions, " AND ")
	if conditionString != "" {
		conditionString = fmt.Sprintf("@if(%s)", conditionString)
	}

	return queries, conditionString, nil
}

func parseQueryIndex(queryIndex string) (sliceIndex int, schemaIndex int, err error) {
	// queryIndex should have the format q_<sliceIndex>_<schemaIndex>
	// e.g: q_0_2
	queryIndexParts := strings.Split(queryIndex, "_")
	if len(queryIndexParts) != 3 {
		// hopefully no unrecognized queries found
		return 0, 0, fmt.Errorf("unrecognized query")
	}

	sliceIndex, err = strconv.Atoi(queryIndexParts[1])
	if err != nil {
		return 0, 0, errors.Wrapf(err, "sliceIndex atoi %s", queryIndex)
	}
	schemaIndex, err = strconv.Atoi(queryIndexParts[2])
	if err != nil {
		return 0, 0, errors.Wrapf(err, "schemaIndex atoi %s", queryIndex)
	}

	return sliceIndex, schemaIndex, nil
}

func (m *mutation) processQuery(assigned *api.Response) error {
	var mapNodes map[string][]json.RawMessage
	if err := json.Unmarshal(assigned.Json, &mapNodes); err != nil {
		return errors.Wrapf(err, `unmarshal queryResponse "%s"`, assigned.Json)
	}

	for queryIndex, msg := range mapNodes {
		if len(msg) == 0 {
			continue
		}

		sliceIndex, schemaIndex, err := parseQueryIndex(queryIndex)
		if err != nil {
			return err
		}

		schema := m.mType.schema[schemaIndex]
		val := m.mType.value.Index(sliceIndex).Elem()
		uidIndex, _ := m.mType.uidIndex()

		if m.returnQuery {
			if m.predicate == "" || m.predicate == schema.Predicate {
				if err := json.Unmarshal(msg[0], val.Addr().Interface()); err != nil {
					return errors.Wrapf(err, "unmarshal query %s", queryIndex)
				}
			}
			continue
		}

		var node node
		if err := json.Unmarshal(msg[0], &node); err != nil {
			return errors.Wrapf(err, "unmarshal node %s", queryIndex)
		}

		queryUID := node.UID

		// set uid if upsert
		if m.predicate != "" && m.predicate == schema.Predicate {
			uidFunc := fmt.Sprintf("uid(u_%d_%d)", sliceIndex, schemaIndex)
			uidField := val.Field(uidIndex)
			if uidFunc == uidField.String() {
				// set the uid, if this is the matching upsert uid
				uidField.SetString(queryUID)
			}
			continue
		}

		// only return unique error if not updating the user specified node
		// i.e: UID field is set
		if val.Field(uidIndex).String() != queryUID {
			return &UniqueError{
				NodeType: m.mType.nodeType,
				Field:    schema.Predicate,
				Value:    val.Field(schemaIndex).Interface(),
				UID:      queryUID,
			}
		}
	}
	return nil
}

// saveUID saves the UID to the passed model, field with uid json tag
func (m *mutation) saveUID(uids map[string]string) error {
	val := m.mType.value

	uidIndex, err := m.mType.uidIndex()
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
			val := el.Field(uidIndex).String()
			if val[0] == '_' {
				// don't return node alias if uid not assigned
				el.Field(uidIndex).SetString("")
			}
		}

		if m.predicate != "" {
			// if upsert created a new node
			schemaIndex := m.mType.predIndex[m.predicate]
			uidFunc := fmt.Sprintf("uid(u_%d_%d)", i, schemaIndex)
			if uid, exists := uids[uidFunc]; exists {
				el.Field(uidIndex).SetString(uid)
			}
		}
	}

	return nil
}

func isNull(x interface{}) bool {
	return x == nil || reflect.DeepEqual(x, reflect.Zero(reflect.TypeOf(x)).Interface())
}

func isTime(refType reflect.Type) bool {
	return refType.PkgPath() == "time"
}

func injectTypeInValue(refVal *reflect.Value) error {
	refType := refVal.Type()
	for i := refVal.NumField() - 1; i >= 0; i-- {
		field := refType.Field(i)
		fieldVal := refVal.Field(i)

		predicate := getPredicate(&field)
		if predicate == dgraphTypePredicate {
			nodeType := GetNodeType(refVal.Interface())
			switch field.Type.Kind() {
			case reflect.String:
				fieldVal.SetString(nodeType)
			case reflect.Slice:
				if field.Type.Elem().Kind() != reflect.String {
					return errors.New(`"dgraph.type" field is not a slice of strings`)
				}
				fieldVal.Set(reflect.ValueOf([]string{nodeType}))
			default:
				return errors.New(`unsupported type for "dgraph.type" predicate`)
			}
		}

		// nested structure, try to traverse
		switch field.Type.Kind() {
		case reflect.Ptr:
			elemType := field.Type.Elem()
			if elemType.Kind() != reflect.Struct || isTime(elemType) {
				continue
			}
		case reflect.Slice:
			elemType := field.Type.Elem()
			if elemType.Kind() == reflect.Ptr {
				elemType = elemType.Elem()
			}
			if elemType.Kind() != reflect.Struct || isTime(elemType) {
				continue
			}
			fieldVal = fieldVal.Addr()
		case reflect.Struct:
			if isTime(field.Type) {
				continue
			}
			fieldVal = fieldVal.Addr()
		default:
			continue
		}
		if err := injectType(fieldVal.Interface()); err != nil {
			return err
		}
	}
	return nil
}

func injectType(data interface{}) error {
	refVal := reflect.ValueOf(data)
	if refVal.Type().Kind() != reflect.Ptr {
		return fmt.Errorf("passed model %s is not a pointer", refVal.Type())
	}

	refVal = refVal.Elem()
	switch refVal.Kind() {
	case reflect.Slice:
		for i := refVal.Len() - 1; i >= 0; i-- {
			elVal := refVal.Index(i)
			if elVal.Kind() == reflect.Ptr {
				elVal = elVal.Elem()
			}
			if err := injectTypeInValue(&elVal); err != nil {
				return err
			}
		}
	case reflect.Struct:
		if err := injectTypeInValue(&refVal); err != nil {
			return err
		}
	}

	return nil
}

func marshalAndInjectType(data interface{}) ([]byte, error) {
	if err := injectType(data); err != nil {
		return nil, errors.Wrap(err, "inject type")
	}
	return json.Marshal(data)
}
