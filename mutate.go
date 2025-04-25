/*
 * Copyright (C) 2020 Dolan and Contributors
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
	stdjson "encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/dgraph-io/dgo/v240/protos/api"
	"github.com/dolan-in/reflectwalk"
	"github.com/pkg/errors"
)

type mutationOpCode uint8

const (
	mutationMutate mutationOpCode = iota
	mutationMutateBasic
	mutationMutateOrGet
	mutationUpsert
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

func isNull(x interface{}) bool {
	return x == nil || reflect.DeepEqual(x, reflect.Zero(reflect.TypeOf(x)).Interface())
}

type node struct {
	UID string `json:"uid"`
}

type mutateType struct {
	uidIndex    int
	schema      []*Schema // maps struct index to dgraph schema
	uidFuncPred string    // types with unique field must have a single predicate that determines the uid func
	nodeType    string
}

func isUIDAlias(uid string) bool {
	return strings.HasPrefix(uid, "_:")
}

func (m *mutateType) getID(v reflect.Value) string {
	id := v.Field(m.uidIndex).String()
	if isUIDAlias(id) {
		return v.Field(m.uidIndex).String()[2:]
	}
	return id
}

func newMutateType(numFields int) *mutateType {
	return &mutateType{
		uidIndex: -1,
		schema:   make([]*Schema, 0, numFields),
	}
}

type preparedMutation struct {
	queries    []string
	conditions []string
	value      map[string]interface{}
}

type mutation struct {
	data         interface{}
	txn          *TxnContext
	mutations    []preparedMutation
	request      api.Request
	queries      []string
	typeCache    map[string]*mutateType
	nodeCache    map[string]reflect.Value
	refCache     map[string]map[string]interface{}
	parentUids   map[string]string
	conditions   map[string][]string
	opcode       mutationOpCode
	upsertFields set
	depth        int
}

func getCreatedUIDs(uidsMap map[string]string) []string {
	uids := make([]string, 0, len(uidsMap))
	for _, uid := range uidsMap {
		uids = append(uids, uid)
	}
	return uids
}

func (m *mutation) mutate() ([]string, error) {
	preHook := generateSchemaHook{mutation: m, skipTyping: true}
	err := reflectwalk.Walk(m.data, preHook)
	if err != nil {
		return nil, errors.Wrap(err, "pre-mutation hook failed")
	}

	setJSON, err := json.Marshal(m.data)
	if err != nil {
		return nil, errors.Wrap(err, "marshal setJSON failed")
	}

	Logger().WithName("dgman").V(3).Info("mutate", "setJSON", string(setJSON))

	resp, err := m.txn.txn.Mutate(m.txn.ctx, &api.Mutation{
		SetJson:   setJSON,
		CommitNow: m.txn.commitNow,
	})
	if err != nil {
		return nil, errors.Wrap(err, "txn mutate failed")
	}

	postHook := setUIDHook{resp: resp}
	err = reflectwalk.Walk(m.data, postHook)
	if err != nil {
		return nil, errors.Wrap(err, "post-mutation hook failed")
	}

	return getCreatedUIDs(resp.Uids), nil
}

func (m *mutation) do() ([]string, error) {
	err := m.generateRequest()
	if err != nil {
		return nil, errors.Wrap(err, "generate request failed")
	}

	Logger().WithName("dgman").V(3).Info("do request", "request", m.request.String())

	resp, err := m.txn.txn.Do(m.txn.ctx, &m.request)
	if err != nil {
		return nil, errors.Wrap(err, "do request failed")
	}

	err = m.processResponse(resp)
	if err != nil {
		return nil, err
	}

	return getCreatedUIDs(resp.Uids), nil
}

func (m *mutation) generateRequest() error {
	preMutationHooks := []reflectwalk.StructWalker{
		generateSchemaHook{mutation: m},
		generateMutationHook{m},
	}
	for i, hook := range preMutationHooks {
		err := reflectwalk.Walk(m.data, hook)
		if err != nil {
			return errors.Wrapf(err, "pre-mutation %d hook failed", i)
		}
	}

	for i, mutation := range m.mutations {
		setJSON, err := json.Marshal(mutation.value)
		if err != nil {
			return errors.Wrapf(err, "marshal mutation value %d failed", i)
		}

		var condition string
		if len(mutation.conditions) > 0 {
			condition = fmt.Sprintf("@if(%s)", strings.Join(mutation.conditions, " AND "))
		}

		m.request.Mutations = append(m.request.Mutations, &api.Mutation{
			SetJson: setJSON,
			Cond:    condition,
		})
	}
	queryString := strings.Join(m.queries, "\n")
	if queryString != "" {
		m.request.Query = fmt.Sprintf("{\n%s\n}", queryString)
	}

	return nil
}

func getElemValue(value reflect.Value) reflect.Value {
	if value.Kind() == reflect.Interface {
		value = value.Elem()
	}
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}
	return value
}

func (m *mutation) setEdgeUID(target map[string]interface{}, edgeValue reflect.Value) {
	edgeValue = getElemValue(edgeValue)
	edgeMutateType := m.typeCache[edgeValue.Type().String()]

	target[predicateUid] = edgeValue.Field(edgeMutateType.uidIndex).String()
}

// addToRefMap adds a reference to an edge, for easier updating reference to edge uids on upsert
func (m *mutation) addToRefMap(edge map[string]interface{}) {
	uid := edge[predicateUid].(string)
	if isUIDAlias(uid) {
		id := uid[2:]
		m.refCache[id] = edge
	}
}

func (m *mutation) addToParentMap(parent, edge map[string]interface{}) {
	parentUID := parent[predicateUid].(string)
	edgeUID := edge[predicateUid].(string)
	if isUIDAlias(edgeUID) {
		id := edgeUID[2:]
		m.parentUids[id] = parentUID
	}
}

// setRefsToUIDFunc sets reference uid alias in edges to UID func on upsert
func (m *mutation) setRefsToUIDFunc(id, uidFunc string) {
	ref, ok := m.refCache[id]
	if ok {
		ref[predicateUid] = uidFunc
	}
}

func (m *mutation) setEdge(nodeValue, edge map[string]interface{}, field reflect.Value) {
	fieldValue := getElemValue(field)
	if !fieldValue.IsValid() {
		return
	}
	edgeType := m.typeCache[fieldValue.Type().String()]
	edgeID := edgeType.getID(fieldValue)
	if isUID(edgeID) {
		copyStructToMap(fieldValue, edge)
	} else {
		m.setEdgeUID(edge, field)
		m.addToRefMap(edge)
		m.addToParentMap(nodeValue, edge)
	}
}

func copyStructToMap(structVal reflect.Value, target map[string]interface{}) {
	for i := 0; i < structVal.NumField(); i++ {
		field := structVal.Field(i)
		jsonTags := strings.Split(structVal.Type().Field(i).Tag.Get("json"), ",")
		if len(jsonTags) == 0 {
			continue
		}
		if len(jsonTags) == 2 && (jsonTags[1] == "omitempty" || jsonTags[1] == "omitzero") && isNull(field.Interface()) {
			continue
		}
		target[jsonTags[0]] = field.Interface()
	}
}

func (m *mutation) copyNodeValues(nodeValue map[string]interface{}, field reflect.Value, schema *Schema, schemaIndex int) {
	switch schema.Type {
	case "[uid]":
		edgesPlaceholder := make([]map[string]interface{}, field.Len(), field.Cap())
		for i := 0; i < field.Len(); i++ {
			fieldEl := field.Index(i)
			edgeEl := map[string]interface{}{}
			m.setEdge(nodeValue, edgeEl, fieldEl)
			edgesPlaceholder[i] = edgeEl
		}
		nodeValue[schema.Predicate] = edgesPlaceholder
	case "uid":
		edge := map[string]interface{}{}
		m.setEdge(nodeValue, edge, field)
		nodeValue[schema.Predicate] = edge
	default:
		if field.CanSet() {
			nodeValue[schema.Predicate] = field.Interface()
		}
	}
}

func generateFilter(id, nodeType, predicate string, jsonValue []byte) string {
	filter := fmt.Sprintf("eq(%s, %s) AND type(%s)", predicate, jsonValue, nodeType)
	if isUID(id) {
		// if update make sure not unique checking the current node
		filter = fmt.Sprintf("NOT uid(%s) AND %s", id, filter)
	}
	return filter
}

// isUpsertField checks if the predicate is an upsert field specified by the user,
// when upsertFields is empty, any unique field can be upserted
func (m *mutation) isUpsertField(predicate string) bool {
	return m.upsertFields.Has(predicate)
}

func (m *mutation) generateQuery(id string, mutateType *mutateType, uidListIndex string, schema *Schema, value interface{}, level int) (query string, err error) {
	queryIndex := fmt.Sprintf("q%s", uidListIndex[1:])

	jsonValue, err := json.Marshal(value)
	if err != nil {
		return "", errors.Wrapf(err, "marshal %v", value)
	}

	filter := generateFilter(id, mutateType.nodeType, schema.Predicate, jsonValue)

	queryFields := fmt.Sprintf("%s as uid", uidListIndex)
	if m.opcode == mutationMutateOrGet {
		var buffer strings.Builder
		expandPredicate(&buffer, m.depth-level)
		queryFields = fmt.Sprintf("%s\n\t\texpand(_all_)%s", queryFields, buffer.String())
	}

	query = fmt.Sprintf("\t%s(func: type(%s), first: 1) @filter(%s) {\n\t\t%s\n\t}", queryIndex, mutateType.nodeType, filter, queryFields)

	return query, nil
}

func (m *mutation) updateToUIDFunc(v reflect.Value, nodeValue map[string]interface{}, id, uidListIndex string, uidIndex int) string {
	uidFunc := fmt.Sprintf("uid(%s)", uidListIndex)
	// update uid value to uid func
	nodeValue[predicateUid] = uidFunc
	v.Field(uidIndex).SetString(uidFunc)
	// update node cache to use uid func instead of uid alias
	m.nodeCache[uidFunc] = v
	// update parent uid
	m.parentUids[uidFunc] = m.parentUids[id]

	m.setRefsToUIDFunc(id, uidFunc)

	return uidFunc
}

func (m *mutation) generateMutation(v reflect.Value, level int) error {
	var (
		queries    []string
		conditions []string
	)

	vType := v.Type()
	mutateType := m.typeCache[vType.String()]

	if mutateType == nil || mutateType.uidIndex == -1 {
		// not a dgraph node struct
		return nil
	}

	id := mutateType.getID(v)
	// use map[string]interface as nodeValue, to prevent including empty values on parent mutations
	nodeValue := make(map[string]interface{}, vType.NumField())
	idFunc := id

	if isUID(id) && level > 0 {
		// adding existing node edges
		return nil
	}

	for schemaIndex, schema := range mutateType.schema {
		field := v.Field(schemaIndex)
		if !field.CanInterface() {
			// probably an unexported field, skip
			continue
		}

		value := field.Interface()
		if schema.OmitEmpty && isNull(value) {
			// empty/null values don't need be to processed
			continue
		}

		// copy values to prevent mutating original data when setting edges
		m.copyNodeValues(nodeValue, field, schema, schemaIndex)

		if schema.Unique {
			uidListIndex := fmt.Sprintf("u_%s_%d", id, schemaIndex)

			isNotUpdate := !isUID(id)
			isUIDFuncField := mutateType.uidFuncPred == schema.Predicate
			if isNotUpdate && isUIDFuncField {
				idFunc = m.updateToUIDFunc(v, nodeValue, id, uidListIndex, mutateType.uidIndex)
			}

			query, err := m.generateQuery(id, mutateType, uidListIndex, schema, value, level)
			if err != nil {
				return errors.Wrapf(err, "generate query on %s field failed", schema.Predicate)
			}

			queries = append(queries, query)

			isAddCondition := m.opcode != mutationUpsert || !isUIDFuncField
			if isAddCondition {
				conditions = append(conditions, fmt.Sprintf("eq(len(%s), 0)", uidListIndex))
			}
		}
	}

	// add parent conditions to prevent orphaned child nodes
	parentConditions := m.conditions[m.parentUids[idFunc]]
	conditions = append(parentConditions, conditions...)
	m.conditions[idFunc] = conditions

	m.mutations = append([]preparedMutation{{
		conditions: conditions,
		value:      nodeValue,
	}}, m.mutations...)
	m.queries = append(m.queries, queries...)

	return nil
}

func parseQueryIndex(queryIndex string) (id string, schemaIndex int, err error) {
	// queryIndex should have the format q_<id>_<schemaIndex>
	// e.g: q_0_2
	queryIndexParts := strings.Split(queryIndex, "_")
	if len(queryIndexParts) != 3 {
		// hopefully no unrecognized queries found
		return "", 0, fmt.Errorf("unrecognized query")
	}

	id = queryIndexParts[1]
	isAlias := !(isUID(id) || isUIDFunc(id))
	if isAlias {
		id = "_:" + id
	}

	schemaIndex, err = strconv.Atoi(queryIndexParts[2])
	if err != nil {
		return "", 0, errors.Wrapf(err, "schemaIndex atoi %s", queryIndex)
	}

	return id, schemaIndex, nil
}

func (m *mutation) processJSONResponse(resp []byte) error {
	var mapNodes map[string][]stdjson.RawMessage
	if err := json.Unmarshal(resp, &mapNodes); err != nil {
		return errors.Wrapf(err, `unmarshal queryResponse "%s"`, resp)
	}

	for queryIndex, msg := range mapNodes {
		if len(msg) == 0 {
			continue
		}

		id, schemaIndex, err := parseQueryIndex(queryIndex)
		if err != nil {
			return err
		}

		nodeValue := m.nodeCache[id]
		mutateType := m.typeCache[nodeValue.Type().String()]
		schema := mutateType.schema[schemaIndex]

		switch m.opcode {
		case mutationMutate:
			var node node
			if err := json.Unmarshal(msg[0], &node); err != nil {
				return errors.Wrapf(err, "unmarshal node %s", queryIndex)
			}

			queryUID := node.UID

			// only return unique error if not updating the user specified node
			// i.e: UID field is set
			if nodeValue.Field(mutateType.uidIndex).String() != queryUID {
				return &UniqueError{
					NodeType: mutateType.nodeType,
					Field:    schema.Predicate,
					Value:    nodeValue.Field(schemaIndex).Interface(),
					UID:      queryUID,
				}
			}
		case mutationMutateOrGet:
			parent := m.nodeCache[m.parentUids[id[2:]]]
			if parent.IsValid() {
				parentType := m.typeCache[parent.Type().String()]
				parentID := parentType.getID(parent)
				if isUID(parentID) {
					// if parent is already set from query, don't unmarshal this query
					continue
				}
			}

			if err := json.Unmarshal(msg[0], nodeValue.Addr().Interface()); err != nil {
				return errors.Wrapf(err, "unmarshal query %s", queryIndex)
			}
		case mutationUpsert:
			// set uid based on existing node query
			var node node
			if err := json.Unmarshal(msg[0], &node); err != nil {
				return errors.Wrapf(err, "unmarshal node %s", queryIndex)
			}

			uidFunc := fmt.Sprintf("uid(u_%s_%d)", id[2:], schemaIndex)
			upsertNodeValue, ok := m.nodeCache[uidFunc]
			if !ok {
				// if not upsert field, return unique error
				return &UniqueError{
					NodeType: mutateType.nodeType,
					Field:    schema.Predicate,
					Value:    nodeValue.Field(schemaIndex).Interface(),
					UID:      node.UID,
				}
			}

			queryUID := node.UID

			uidField := upsertNodeValue.Field(mutateType.uidIndex)
			if uidFunc == uidField.String() {
				uidField.SetString(queryUID)
			}
		}
	}

	return nil
}

func (m *mutation) processResponse(resp *api.Response) error {
	if resp.Json != nil {
		if err := m.processJSONResponse(resp.Json); err != nil {
			return err
		}
	}

	postHook := setUIDHook{resp: resp}
	err := reflectwalk.Walk(m.data, postHook)
	if err != nil {
		return errors.Wrap(err, "post-mutation hook failed")
	}

	return nil
}

func setType(field reflect.StructField, fieldVal reflect.Value, nodeType string) error {
	if !fieldVal.CanSet() {
		return fmt.Errorf("dgraph.type not settable on %s.%s", nodeType, field.Name) // did you pass pointer?
	}
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

	return nil
}

type generateSchemaHook struct {
	mutation   *mutation
	skipTyping bool
}

func (h generateSchemaHook) Struct(v reflect.Value, level int) error {
	return nil
}

func (h generateSchemaHook) StructField(p reflect.Value, field reflect.StructField, v reflect.Value, level int) error {
	if !v.CanInterface() {
		// unexported field, skip
		return nil
	}

	pType := p.Type()
	nodeType := pType.Name()
	mutateType, ok := h.mutation.typeCache[pType.String()]
	if !ok {
		mutateType = newMutateType(p.NumField())
	}
	// schema typing is completed before on type
	skipTyping := p.NumField() > 0 && len(mutateType.schema) == p.NumField()
	if h.skipTyping {
		skipTyping = true
	}

	i := field.Index[len(field.Index)-1]
	fieldName := fmt.Sprintf("%s.%s", pType.Name(), field.Name)

	predicate, _ := getPredicate(&field)
	switch predicate {
	case predicateUid:
		uid, err := genUID(field, v)
		if err != nil {
			return errors.Wrap(err, "gen UID failed")
		}
		if uid != "" {
			// cache the struct value by its generated id
			h.mutation.nodeCache[uid] = p
		}
		// for easier accessing the uid field
		mutateType.uidIndex = i
	case predicateDgraphType:
		dgraphTag := field.Tag.Get(tagName)
		if dgraphTag != "" {
			nodeType = dgraphTag
		}
		if err := setType(field, v, nodeType); err != nil {
			return errors.Wrapf(err, "set type failed on %s", fieldName)
		}
		mutateType.nodeType = nodeType

		// is a dgraph node, set max level as depth
		if level > h.mutation.depth {
			// set max level as depth
			h.mutation.depth = level
		}
	}

	if !skipTyping {
		schema, err := parseDgraphTag(&field)
		if err != nil {
			return errors.Wrapf(err, "parse dgraph tag failed on %s", fieldName)
		}
		mutateType.schema = append(mutateType.schema, schema)
		if schema.Unique {
			if h.mutation.upsertFields.Has(predicate) {
				mutateType.uidFuncPred = predicate
			}

			if mutateType.uidFuncPred == "" {
				mutateType.uidFuncPred = predicate
			}
		}
		// cache the parsed type
		h.mutation.typeCache[pType.String()] = mutateType

		return nil
	}

	return nil
}

type generateMutationHook struct {
	mutation *mutation
}

func (h generateMutationHook) Struct(v reflect.Value, level int) error {
	return h.mutation.generateMutation(v, level)
}

func (h generateMutationHook) StructField(s reflect.Value, f reflect.StructField, v reflect.Value, level int) error {
	return nil
}

type setUIDHook struct {
	resp *api.Response
}

func (h setUIDHook) Struct(v reflect.Value, level int) error {
	return nil
}

func (h setUIDHook) StructField(s reflect.Value, f reflect.StructField, v reflect.Value, level int) error {
	err := setUIDs(f, v, h.resp.Uids)
	if err != nil {
		return errors.Wrap(err, "set UIDs failed")
	}
	return nil
}

func newMutation(txn *TxnContext, data interface{}) *mutation {
	return &mutation{
		data: data,
		txn:  txn,
		// TODO: optimize use of maps
		nodeCache:  make(map[string]reflect.Value),
		typeCache:  make(map[string]*mutateType),
		refCache:   make(map[string]map[string]interface{}),
		conditions: make(map[string][]string),
		parentUids: make(map[string]string),
		request: api.Request{
			CommitNow: txn.commitNow,
		},
	}
}

// SetTypes recursively walks all structures in data and sets the value of the
// `dgraph.type` struct field. The type, in order of preference, is either the
// value of the `dgraph` struct tag on the `dgraph.type` struct field, or the
// struct name.
// Courtesy of @freb
func SetTypes(data interface{}) error {
	return reflectwalk.Walk(data, typeWalker{})
}

type typeWalker struct{}

func (w typeWalker) Struct(v reflect.Value, level int) error {
	vType := v.Type()
	nodeType := vType.Name()
	numFields := v.NumField()

	for i := numFields - 1; i >= 0; i-- {
		field := vType.Field(i)
		fieldVal := v.Field(i)
		fieldName := fmt.Sprintf("%s.%s", vType.Name(), field.Name)

		predicate, _ := getPredicate(&field)
		if predicate == predicateDgraphType {
			dgraphTag := field.Tag.Get(tagName)
			if dgraphTag != "" {
				nodeType = dgraphTag
			}
			if err := setType(field, fieldVal, nodeType); err != nil {
				return errors.Wrapf(err, "set type failed on %s", fieldName)
			}
		}
	}
	return nil
}

func (w typeWalker) StructField(s reflect.Value, f reflect.StructField, v reflect.Value, level int) error {
	return nil
}
