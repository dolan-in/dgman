/*
 * Copyright (C) 2019 Dolan and Contributors
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

	"github.com/dgraph-io/dgo/v200"
)

// TxnInterface provides interface for dgman.TxnContext
type TxnInterface interface {
	Commit() error
	Discard() error
	BestEffort() *TxnContext
	Txn() *dgo.Txn
	WithContext(context.Context)
	Context() context.Context
	Mutate(data interface{}, commitNow ...bool) error
	Create(data interface{}, commitNow ...bool) error
	Update(data interface{}, commitNow ...bool) error
	Upsert(data interface{}, predicate string, commitNow ...bool) error
	CreateOrGet(data interface{}, predicate string, commitNow ...bool) error
	Delete(model interface{}, commitNow ...bool) *Deleter
	Get(model interface{}) *Query
}

// SchemaType allows defining a custom type as a dgraph schema type
type SchemaType interface {
	SchemaType() string
}

var (
	_ TxnInterface = (*TxnContext)(nil)
)
