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
	"math/big"
	"strconv"
	"time"
	"unsafe"

	"github.com/dgraph-io/dgo/v240"
	"github.com/dgraph-io/dgo/v240/protos/api"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.Config{
	EscapeHTML:             true,
	SortMapKeys:            true,
	ValidateJsonRawMessage: true,
}.Froze()

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

type bigFloatEncoder struct{}

func (e *bigFloatEncoder) IsEmpty(ptr unsafe.Pointer) bool {
	f := (*big.Float)(ptr)
	return f == nil || f.Sign() == 0
}

func (e *bigFloatEncoder) Encode(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	f := (*big.Float)(ptr)
	if f == nil || f.Sign() == 0 {
		stream.WriteNil()
	} else {
		stream.WriteString(f.Text('f', -1))
	}
}

type bigFloatDecoder struct{}

func (d *bigFloatDecoder) Decode(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	switch iter.WhatIsNext() {
	case jsoniter.NilValue:
		iter.ReadNil()
		*(*big.Float)(ptr) = *big.NewFloat(0)
	case jsoniter.StringValue:
		str := iter.ReadString()
		f, _, err := big.ParseFloat(str, 10, 0, big.ToNearestEven)
		if err != nil {
			iter.ReportError("decode big.Float", err.Error())
			return
		}
		*(*big.Float)(ptr) = *f
	case jsoniter.NumberValue:
		str := iter.ReadNumber().String()
		f, _, err := big.ParseFloat(str, 10, 0, big.ToNearestEven)
		if err != nil {
			iter.ReportError("decode big.Float", err.Error())
			return
		}
		*(*big.Float)(ptr) = *f
	default:
		iter.ReportError("decode big.Float", "invalid value type")
	}
}

func init() {
	jsoniter.RegisterTypeEncoder("time.Time", &timeEncoder{})
	jsoniter.RegisterTypeEncoder("big.Float", &bigFloatEncoder{})
	jsoniter.RegisterTypeDecoder("big.Float", &bigFloatDecoder{})
}
