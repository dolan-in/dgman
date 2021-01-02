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
	"fmt"
	"reflect"
	"sync/atomic"

	"github.com/dolan-in/reflectwalk"
)

// overflow is OK
var blankuid int32 = 0

func blankUID() string {
	i := atomic.AddInt32(&blankuid, 1)
	return fmt.Sprintf("_:%d", i)
}

func genUID(f reflect.StructField, v reflect.Value) (string, error) {
	if v.Kind() != reflect.String {
		return "", nil
	}

	predicate := getPredicate(&f)
	uid := v.String()

	if predicate == "uid" {
		if uid != "" {
			// if uid already set, don't generate
			return uid, nil
		}
		if !v.CanSet() {
			return "", fmt.Errorf("cannot set uid")
		}
		uid = blankUID()
		v.Set(reflect.ValueOf(uid))
		return uid, nil
	}
	return "", nil
}

func setUIDs(f reflect.StructField, v reflect.Value, uids map[string]string) error {
	if v.Kind() != reflect.String {
		return nil
	}

	predicate := getPredicate(&f)
	setUID := v.String()

	if predicate != "uid" {
		return nil
	}

	if !v.CanSet() {
		return fmt.Errorf("cannot set %s/%s", predicate, setUID)
	}

	if isUIDAlias(setUID) {
		uid, ok := uids[setUID[2:]]
		if ok {
			v.SetString(uid)
		}
	} else if isUIDFunc(setUID) {
		uid, ok := uids[setUID]
		if ok {
			v.SetString(uid)
		}
	}

	return nil
}

// SetUIDs recursively walks all structures in data and sets the value of the
// `uid` struct field based on the uids map. A map of Uids is returned in Dgraph
// mutate calls.
func SetUIDs(data interface{}, uids map[string]string) error {
	w := setUIDWalker{uids: uids}
	return reflectwalk.Walk(data, w)
}

type setUIDWalker struct {
	uids map[string]string
}

func (w setUIDWalker) Struct(v reflect.Value, level int) error {
	return nil
}

func (w setUIDWalker) StructField(f reflect.StructField, v, p reflect.Value, level int) error {
	return setUIDs(f, v, w.uids)
}
