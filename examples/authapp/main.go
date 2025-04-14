package main

import (
	"log"
	"net/http"

	"github.com/dgraph-io/dgo/v250"
	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/dolan-in/dgman/v2"
	"google.golang.org/grpc"
)

func newDgraphClient() dgo.Client {
	d, err := grpc.Dial("localhost:9080", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	return dgo.NewDgraphClient(
		api.NewDgraphClient(d),
	)
}

func newApi(dgoClient dgo.Client) *userAPI {
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

	router := http.NewServeMux()
	router.HandleFunc("/register", api.Register)
	router.HandleFunc("/auth", api.Login)

	server := http.Server{
		Addr:    ":4000",
		Handler: router,
	}
	log.Fatal(server.ListenAndServe())
}
