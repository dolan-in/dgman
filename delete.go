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
	"bytes"
	"log"

	"github.com/dgraph-io/dgo/v200/protos/api"
	"github.com/pkg/errors"
)

type DeleteQuery struct {
	query  *QueryBlock
	result []byte
}

// Scan will unmarshal the delete query result into the passed interface{},
// if nothing is passed, it will be unmarshaled to the individual query models.
func (d *DeleteQuery) Scan(dst ...interface{}) error {
	return d.query.scan(d.result, dst...)
}

// DeleteParams is a struct to past delete parameters
type DeleteParams struct {
	Cond  string
	Nodes []DeleteNode
}

// DeleteNode is a struct to build delete node n-quads
type DeleteNode struct {
	UID   string
	Edges []DeleteEdge
}

func (d *DeleteNode) writeTo(buffer *bytes.Buffer) {
	if len(d.Edges) == 0 {
		// delete node
		writeUID(buffer, d.UID)
		buffer.WriteString("* * .\n")
		return
	}

	for _, edge := range d.Edges {
		edge.writeTo(buffer, d.UID)
	}
}

type DeleteEdge struct {
	Pred string
	UIDs []string
}

func (d *DeleteEdge) writeTo(buffer *bytes.Buffer, uid string) {
	if len(d.UIDs) == 0 {
		// delete all edges
		writeUID(buffer, uid)
		writeIRI(buffer, d.Pred)
		buffer.WriteString("* .\n")
		return
	}

	for _, edgeUID := range d.UIDs {
		// subject
		writeUID(buffer, uid)
		// predicate
		writeIRI(buffer, d.Pred)
		// object
		writeUID(buffer, edgeUID)
		buffer.WriteString(".\n")
	}
}

func writeIRI(w *bytes.Buffer, iri string) {
	w.WriteString("<")
	w.WriteString(iri)
	w.WriteString("> ")
}

func writeUID(w *bytes.Buffer, uid string) {
	if isUID(uid) {
		writeIRI(w, uid)
	} else {
		w.WriteString("uid(")
		w.WriteString(uid)
		w.WriteString(") ")
	}
}

func (d *TxnContext) delete(params ...*DeleteParams) error {
	_, err := d.deleteQuery(nil, params...)
	return err
}

func (d *TxnContext) deleteQuery(query *QueryBlock, params ...*DeleteParams) (DeleteQuery, error) {
	mutations := make([]*api.Mutation, len(params))
	for i, param := range params {
		var nQuads bytes.Buffer
		for _, node := range param.Nodes {
			node.writeTo(&nQuads)
		}
		log.Println(nQuads.String())
		mutations[i] = &api.Mutation{
			DelNquads: nQuads.Bytes(),
			Cond:      param.Cond,
		}
	}
	req := &api.Request{
		Mutations: mutations,
		CommitNow: d.commitNow,
	}
	if query != nil {
		req.Query = query.String()
	}
	resp, err := d.txn.Do(d.ctx, req)
	if err != nil {
		return DeleteQuery{}, errors.Wrap(err, "request failed")
	}
	return DeleteQuery{
		query:  query,
		result: resp.Json,
	}, nil
}

func (d *TxnContext) deleteNode(uids ...string) error {
	var nQuads bytes.Buffer
	for _, uid := range uids {
		writeIRI(&nQuads, uid)
		nQuads.WriteString("* * .\n")
	}
	_, err := d.txn.Mutate(d.ctx, &api.Mutation{
		DelNquads: nQuads.Bytes(),
		CommitNow: d.commitNow,
	})
	return err
}

func (d *TxnContext) deleteEdge(uid string, predicate string, edgeUIDs ...string) error {
	var nQuads bytes.Buffer
	if len(edgeUIDs) > 0 {
		for _, edgeUID := range edgeUIDs {
			writeIRI(&nQuads, uid)
			writeIRI(&nQuads, predicate)
			writeIRI(&nQuads, edgeUID)
			nQuads.WriteString(".\n")
		}
	} else {
		writeIRI(&nQuads, uid)
		writeIRI(&nQuads, predicate)
		nQuads.WriteString("* .\n")
	}
	_, err := d.txn.Mutate(d.ctx, &api.Mutation{
		DelNquads: nQuads.Bytes(),
		CommitNow: d.commitNow,
	})
	return err
}
