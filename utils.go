package dgman

import (
	"context"
	"log"
	"regexp"
	"strings"

	"github.com/dgraph-io/dgo"

	"github.com/dgraph-io/dgo/protos/api"
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
		log.Fatal(err)
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
		log.Fatal(err)
	}
}
