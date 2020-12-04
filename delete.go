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

	"github.com/dgraph-io/dgo/v200/protos/api"
	"github.com/pkg/errors"
)

// DeleteCond is a struct to past delete conditions on specified uids
type DeleteCond struct {
	Cond string
	Uids []string
}

type DeleteQuery struct {
	query  *QueryBlock
	result []byte
}

// Scan will unmarshal the delete query result into the passed interface{},
// if nothing is passed, it will be unmarshaled to the individual query models.
func (d DeleteQuery) Scan(dst ...interface{}) error {
	return d.query.scan(d.result, dst...)
}

func (d *TxnContext) deleteQuery(query *QueryBlock, uids ...string) (DeleteQuery, error) {
	var nQuads bytes.Buffer
	for _, uid := range uids {
		if isUID(uid) {
			nQuads.WriteString("<")
			nQuads.WriteString(uid)
			nQuads.WriteString("> * * .\n")
		} else {
			nQuads.WriteString("uid(")
			nQuads.WriteString(uid)
			nQuads.WriteString(") * * .\n")
		}
	}
	req := &api.Request{
		Query: query.String(),
		Mutations: []*api.Mutation{{
			DelNquads: nQuads.Bytes(),
		}},
		CommitNow: d.commitNow,
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

func (d *TxnContext) deleteQueryCondition(query *QueryBlock, conds ...DeleteCond) (DeleteQuery, error) {
	req := &api.Request{
		Query:     query.String(),
		CommitNow: d.commitNow,
	}
	for _, cond := range conds {
		var nQuads bytes.Buffer
		for _, uid := range cond.Uids {
			if isUID(uid) {
				nQuads.WriteString("<")
				nQuads.WriteString(uid)
				nQuads.WriteString("> * * .\n")
			} else {
				nQuads.WriteString("uid(")
				nQuads.WriteString(uid)
				nQuads.WriteString(") * * .\n")
			}
		}
		req.Mutations = append(req.Mutations, &api.Mutation{
			Cond:      cond.Cond,
			DelNquads: nQuads.Bytes(),
		})
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
		nQuads.WriteString("<")
		nQuads.WriteString(uid)
		nQuads.WriteString("> * * .\n")
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
			nQuads.WriteRune('<')
			nQuads.WriteString(uid)
			nQuads.WriteString("> <")
			nQuads.WriteString(predicate)
			nQuads.WriteString("> <")
			nQuads.WriteString(edgeUID)
			nQuads.WriteString("> .\n")
		}
	} else {
		nQuads.WriteRune('<')
		nQuads.WriteString(uid)
		nQuads.WriteString("> <")
		nQuads.WriteString(predicate)
		nQuads.WriteString("> * .\n")
	}
	_, err := d.txn.Mutate(d.ctx, &api.Mutation{
		DelNquads: nQuads.Bytes(),
		CommitNow: d.commitNow,
	})
	return err
}
