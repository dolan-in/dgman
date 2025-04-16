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
	"strconv"
	"time"
	"unsafe"

	"github.com/dgraph-io/dgo/v240"
	"github.com/dgraph-io/dgo/v240/protos/api"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func newDgraphClient() *dgo.Dgraph {
	client, err := dgo.Open("dgraph://localhost:9080")
	if err != nil {
		panic(err)
	}

	return client
}

func dropAll(client ...*dgo.Dgraph) {
	var c *dgo.Dgraph
	if len(client) > 0 {
		c = client[0]
	} else {
		c = newDgraphClient()
	}

	err := c.Alter(context.Background(), &api.Operation{DropAll: true})
	if err != nil {
		panic(err)
	}
}

func intToBytes(no int) []byte {
	return []byte(strconv.Itoa(no))
}

// Set implementation

var exists = struct{}{}

type set map[string]struct{}

func newSet(values ...string) set {
	s := set{}
	// add initial values
	for _, value := range values {
		s.Add(value)
	}
	return s
}

func (s set) Add(value string) {
	s[value] = exists
}

func (s set) Remove(value string) {
	delete(s, value)
}

func (s set) Has(value string) bool {
	_, c := s[value]
	return c
}

// timeEncoder is a custom JSON encoder for time.Time values
type timeEncoder struct{}

func (e *timeEncoder) IsEmpty(ptr unsafe.Pointer) bool {
	return (*time.Time)(ptr).IsZero()
}

// Encode encodes a time.Time value as a JSON string if not a "Zero" time
func (e *timeEncoder) Encode(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	t := *(*time.Time)(ptr)
	if t.IsZero() {
		stream.WriteNil()
	} else {
		stream.WriteString(t.Format(time.RFC3339))
	}
}

func init() {
	jsoniter.RegisterTypeEncoder("time.Time", &timeEncoder{})
}
