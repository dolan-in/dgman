package dgman

import (
	stdjson "encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/dgraph-io/dgo/v200/protos/api"
	"github.com/mitchellh/reflectwalk"
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
	uidIndex int
	schema   []*Schema // maps struct index to dgraph schema
	nodeType string
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
		schema: make([]*Schema, numFields),
	}
}

type preparedMutation struct {
	queries   []string
	condition string
	value     reflect.Value
}

type mutation struct {
	data         interface{}
	txn          *TxnContext
	mutations    []preparedMutation
	request      api.Request
	queries      []string
	typeCache    map[string]*mutateType
	nodeCache    map[string]reflect.Value
	refCache     map[string][]reflect.Value
	opcode       mutationOpCode
	upsertFields set
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

	resp, err := m.txn.txn.Mutate(m.txn.ctx, &api.Mutation{
		SetJson:   setJSON,
		CommitNow: m.txn.commitNow,
	})

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
		setJSON, err := json.Marshal(mutation.value.Interface())
		if err != nil {
			return errors.Wrapf(err, "marshal mutation value %d failed", i)
		}

		m.request.Mutations = append(m.request.Mutations, &api.Mutation{
			SetJson: setJSON,
			Cond:    mutation.condition,
		})
	}
	queryString := strings.Join(m.queries, "\n")
	if queryString != "" {
		m.request.Query = fmt.Sprintf("{\n%s\n}", queryString)
	}

	return nil
}

func setEdgeUID(target, edgeValue reflect.Value) {
	if edgeValue.Kind() == reflect.Ptr {
		edgeValue = edgeValue.Elem()
	}
	edgeType := edgeValue.Type()
	newEdgeValuePtr := reflect.New(edgeType)
	newEdgeValue := newEdgeValuePtr.Elem()
	for i := 0; i < edgeValue.NumField(); i++ {
		field := newEdgeValue.Field(i)
		fieldType := edgeType.Field(i)
		if predicate := getPredicate(&fieldType); predicate == predicateUid {
			field.Set(edgeValue.Field(i))
			break
		}
	}

	if target.Kind() == reflect.Ptr {
		target.Set(newEdgeValuePtr)
	} else {
		target.Set(newEdgeValue)
	}
}

// addToRefMap adds a reference to an edge, for easier updating reference to edge uids on upsert
func (m *mutation) addToRefMap(edge reflect.Value) {
	if edge.Kind() == reflect.Ptr {
		edge = edge.Elem()
	}
	edgeType := m.typeCache[edge.Type().String()]
	uid := edge.Field(edgeType.uidIndex).String()
	if isUIDAlias(uid) {
		id := uid[2:]
		m.refCache[id] = append(m.refCache[id], edge)
	}
}

// setRefsToUIDFunc sets reference uid alias in edges to UID func on upsert
func (m *mutation) setRefsToUIDFunc(id, uidFunc string) {
	refs := m.refCache[id]
	for _, ref := range refs {
		refType := m.typeCache[ref.Type().String()]
		ref.Field(refType.uidIndex).SetString(uidFunc)
	}
}

func (m *mutation) generateMutation(v reflect.Value) error {
	var (
		queries    []string
		conditions []string
	)

	vType := v.Type()
	mutateType := m.typeCache[vType.String()]
	id := mutateType.getID(v)
	nodeValue := reflect.New(vType)
	nodeValue = nodeValue.Elem()
	uniqueCheck := true

	for schemaIndex, schema := range mutateType.schema {
		field := v.Field(schemaIndex)
		if !field.CanInterface() {
			// probably an unexported field, skip
			continue
		}

		value := field.Interface()
		if isNull(value) {
			// empty/null values don't need be to processed
			continue
		}

		// copy values to prevent mutating original data when setting edges
		switch schema.Type {
		case "[uid]":
			edgesPlaceholder := reflect.MakeSlice(field.Type(), field.Len(), field.Cap())
			edgesField := nodeValue.Field(schemaIndex)
			edgesField.Set(edgesPlaceholder)
			for i := 0; i < field.Len(); i++ {
				setEdgeUID(edgesField.Index(i), field.Index(i))
				if m.opcode == mutationUpsert {
					m.addToRefMap(edgesField.Index(i))
				}
			}
		case "uid":
			edge := nodeValue.Field(schemaIndex)
			setEdgeUID(edge, field)
			if m.opcode == mutationUpsert {
				m.addToRefMap(edge)
			}
		default:
			if field.CanSet() {
				nodeValue.Field(schemaIndex).Set(field)
			}
		}

		queryIndex := fmt.Sprintf("q_%s_%d", id, schemaIndex)
		uidListIndex := fmt.Sprintf("u_%s_%d", id, schemaIndex)
		// check if current predicate is an upsert field specified by the user,
		// when upsertFields is empty, any unique field can be upserted
		isUpsertField := len(m.upsertFields) == 0 || m.upsertFields.Has(schema.Predicate)

		if schema.Unique && uniqueCheck {
			jsonValue, err := json.Marshal(value)
			if err != nil {
				return errors.Wrapf(err, "marshal %v", value)
			}

			filter := fmt.Sprintf("eq(%s, %s)", schema.Predicate, jsonValue)
			if isUID(id) {
				// if update make sure not unique checking the current node
				filter = fmt.Sprintf("NOT uid(%s) AND %s", id, filter)
			}

			queryFields := fmt.Sprintf("%s as uid", uidListIndex)
			createOrGet := m.opcode == mutationMutateOrGet && isUpsertField
			if createOrGet {
				queryFields = fmt.Sprintf("%s\nexpand(_all_)", queryFields)
			}

			queries = append(queries, fmt.Sprintf("\t%s(func: type(%s), first: 1) @filter(%s) {\n\t\t%s\n\t}", queryIndex, mutateType.nodeType, filter, queryFields))

			// if upsert, allow mutation to continue, don't include condition to skip mutation
			upsert := m.opcode == mutationUpsert && isUpsertField
			if upsert {
				uidFunc := fmt.Sprintf("uid(%s)", uidListIndex)
				// update uid value to uid func
				nodeValue.Field(mutateType.uidIndex).SetString(uidFunc)
				v.Field(mutateType.uidIndex).SetString(uidFunc)
				// update node cache to use uid func instead of uid alias
				m.nodeCache[uidFunc] = v
				delete(m.nodeCache, id)

				m.setRefsToUIDFunc(id, uidFunc)
				// don't continue unique checking on other fields, because uid can only be set once
				uniqueCheck = false
				continue
			}

			conditions = append(conditions, fmt.Sprintf("eq(len(%s), 0)", uidListIndex))
		}
	}

	conditionString := strings.Join(conditions, " AND ")
	if conditionString != "" {
		conditionString = fmt.Sprintf("@if(%s)", conditionString)
	}

	m.mutations = append([]preparedMutation{{
		condition: conditionString,
		value:     nodeValue,
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
	isAlias := !(isUID(id) && isUIDFunc(id))
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
			if len(m.upsertFields) > 0 && !m.upsertFields.Has(schema.Predicate) {
				// not the specified field, skip
				continue
			}

			if err := json.Unmarshal(msg[0], nodeValue.Addr().Interface()); err != nil {
				return errors.Wrapf(err, "unmarshal query %s", queryIndex)
			}
		case mutationUpsert:
			if len(m.upsertFields) > 0 && !m.upsertFields.Has(schema.Predicate) {
				// not the specified field, skip
				continue
			}

			// set uid based on existing node query
			var node node
			if err := json.Unmarshal(msg[0], &node); err != nil {
				return errors.Wrapf(err, "unmarshal node %s", queryIndex)
			}

			uidFunc := fmt.Sprintf("uid(u_%s_%d)", id[2:], schemaIndex)
			nodeValue = m.nodeCache[uidFunc]
			queryUID := node.UID

			uidField := nodeValue.Field(mutateType.uidIndex)
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

func (h generateSchemaHook) Struct(v reflect.Value) error {
	vType := v.Type()
	nodeType := vType.Name()
	numFields := v.NumField()
	mutateType, skipTyping := h.mutation.typeCache[nodeType]
	if mutateType == nil {
		mutateType = newMutateType(numFields)
	}
	if h.skipTyping {
		skipTyping = true
	}

	for i := numFields - 1; i >= 0; i-- {
		field := vType.Field(i)
		fieldVal := v.Field(i)
		fieldName := fmt.Sprintf("%s.%s", vType.Name(), field.Name)

		predicate := getPredicate(&field)
		switch predicate {
		case predicateUid:
			uid, err := genUID(field, fieldVal)
			if err != nil {
				return errors.Wrap(err, "gen UID failed")
			}
			if uid != "" {
				// cache the struct value by its generated id
				h.mutation.nodeCache[uid] = v
			}
			// for easier accessing the uid field
			mutateType.uidIndex = i
		case predicateDgraphType:
			dgraphTag := field.Tag.Get(tagName)
			if dgraphTag != "" {
				nodeType = dgraphTag
			}
			if err := setType(field, fieldVal, nodeType); err != nil {
				return errors.Wrapf(err, "set type failed on %s", fieldName)
			}
			mutateType.nodeType = nodeType
		}

		if skipTyping {
			continue
		}
		schema, err := parseDgraphTag(&field)
		if err != nil {
			return errors.Wrapf(err, "parse dgraph tag failed on %s", fieldName)
		}
		mutateType.schema[i] = schema
	}
	if !skipTyping {
		// cache the parsed type
		h.mutation.typeCache[vType.String()] = mutateType
	}

	return nil
}

func (h generateSchemaHook) StructField(f reflect.StructField, v reflect.Value) error {
	return nil
}

type generateMutationHook struct {
	mutation *mutation
}

func (h generateMutationHook) Struct(v reflect.Value) error {
	return h.mutation.generateMutation(v)
}

func (h generateMutationHook) StructField(f reflect.StructField, v reflect.Value) error {
	return nil
}

type setUIDHook struct {
	mutation *mutation
	resp     *api.Response
}

func (h setUIDHook) Struct(v reflect.Value) error {
	return nil
}

func (h setUIDHook) StructField(f reflect.StructField, v reflect.Value) error {
	err := setUIDs(f, v, h.resp.Uids)
	if err != nil {
		return errors.Wrap(err, "set UIDs failed")
	}
	return nil
}

func newMutation(txn *TxnContext, data interface{}) *mutation {
	return &mutation{
		data:      data,
		txn:       txn,
		nodeCache: make(map[string]reflect.Value),
		typeCache: make(map[string]*mutateType),
		refCache:  make(map[string][]reflect.Value),
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

func (w typeWalker) Struct(v reflect.Value) error {
	vType := v.Type()
	nodeType := vType.Name()
	numFields := v.NumField()

	for i := numFields - 1; i >= 0; i-- {
		field := vType.Field(i)
		fieldVal := v.Field(i)
		fieldName := fmt.Sprintf("%s.%s", vType.Name(), field.Name)

		predicate := getPredicate(&field)
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

func (w typeWalker) StructField(f reflect.StructField, v reflect.Value) error {
	return nil
}
