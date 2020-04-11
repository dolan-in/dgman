package main

import (
	"log"

	"github.com/dgraph-io/dgo/v200"
	"github.com/dgraph-io/dgo/v200/protos/api"
	"github.com/dolan-in/dgman"
	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
)

func newDgraphClient() *dgo.Dgraph {
	d, err := grpc.Dial("localhost:9080", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	return dgo.NewDgraphClient(
		api.NewDgraphClient(d),
	)
}

func newApi(dgoClient *dgo.Dgraph) *userAPI {
	return &userAPI{
		store: &userStore{c: dgoClient},
	}
}

func main() {
	dg := newDgraphClient()

	schema, err := dgman.CreateSchema(dg, &User{})
	if err != nil {
		log.Fatalln("create schema", err)
	}

	log.Println(schema)

	api := newApi(dg)

	server := gin.New()
	server.POST("/register", api.Register)
	server.POST("/auth", api.Login)
	server.Run(":4000")
}
