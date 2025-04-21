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
	"os"
	"strconv"

	"github.com/dgraph-io/dgo/v240"
	"github.com/dgraph-io/dgo/v240/protos/api"
)

func newDgraphClient() *dgo.Dgraph {
	addr := os.Getenv("DGMAN_TEST_DATABASE")
	if addr == "" {
		addr = "localhost:9080"
	}
	addr = "dgraph://" + addr
	client, err := dgo.Open(addr)
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
