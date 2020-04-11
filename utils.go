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
	"regexp"
	"strconv"
	"strings"

	"github.com/dgraph-io/dgo/v200"

	"github.com/dgraph-io/dgo/v200/protos/api"
	"google.golang.org/grpc"
)

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

func toSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

func newDgraphClient() *dgo.Dgraph {
	d, err := grpc.Dial("localhost:9080", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	return dgo.NewDgraphClient(
		api.NewDgraphClient(d),
	)
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
