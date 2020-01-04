package dgman

// import (
// 	"context"
// 	"log"
// 	"reflect"

// 	"github.com/dgraph-io/dgo/v2"
// )

// // OnConflictCallback defines the callback to update the data when an existing node matches the unique key
// type OnConflictCallback func(uniqueErr UniqueError, found, excluded interface{}) (updated interface{})

// type MultiError struct {
// 	Errors []error
// }

// func (e *MultiError) Collect(err error) {
// 	e.Errors = append(e.Errors, err)
// }

// func (e *MultiError) Error() (errorStr string) {
// 	for _, err := range e.Errors {
// 		errorStr += err.Error() + "\n"
// 	}
// 	return errorStr
// }

// func createWithOnConflictCallback(ctx context.Context, tx *dgo.Txn, mType *mutateType, val *reflect.Value, opt *MutateOptions, cb OnConflictCallback) error {
// 	// copy the node values, for unique checking
// 	refExisting := reflect.New(mType.vType)
// 	refExisting.Elem().Set(val.Elem())
// 	data := refExisting.Interface()

// 	if err := mType.unique(ctx, tx, data, false); err != nil {
// 		uniqueErr, ok := err.(UniqueError)
// 		if !ok {
// 			return err
// 		}

// 		updated := cb(uniqueErr, data, val.Interface())
// 		if updated == nil {
// 			// don't update, return the existing node
// 			val.Set(refExisting)
// 			return nil
// 		}

// 		// update the node
// 		data = updated
// 		val.Elem().Set(reflect.ValueOf(updated).Elem())
// 	}

// 	assigned, err := mutate(ctx, tx, data, opt)
// 	if err != nil {
// 		return err
// 	}

// 	return mType.saveUID(assigned.Uids, val)
// }

// func createOrUpdate(ctx context.Context, tx *dgo.Txn, mType *mutateType, opt *MutateOptions, cb OnConflictCallback) error {
// 	if mType.value.Type().Kind() == reflect.Slice {
// 		errors := new(MultiError)

// 		valLen := mType.value.Len()
// 		for i := 0; i < valLen; i++ {
// 			v := mType.value.Index(i)

// 			if err := createWithOnConflictCallback(ctx, tx, mType, &v, opt, cb); err != nil {
// 				errors.Collect(err)
// 			}
// 		}

// 		if len(errors.Errors) == 0 {
// 			return nil
// 		}

// 		return errors
// 	}
// 	// get the pointer to make it addressable
// 	ptr := mType.value.Addr()
// 	return createWithOnConflictCallback(ctx, tx, mType, &ptr, opt, cb)
// }

// // CreateOrGet creates a node, or returns a node if it exists with a certain unique key
// func CreateOrGet(ctx context.Context, tx *dgo.Txn, data interface{}, options ...MutateOptions) error {
// 	opt := MutateOptions{}
// 	if len(options) > 0 {
// 		opt = options[0]
// 	}

// 	mType, err := newMutateType(data)
// 	if err != nil {
// 		return err
// 	}

// 	cb := func(uniqueErr UniqueError, found, excluded interface{}) interface{} {
// 		log.Println("skipped: ", uniqueErr)
// 		return nil
// 	}

// 	return createOrUpdate(ctx, tx, mType, &opt, cb)
// }

// // Upsert updates the node when a node with a certain unique key exists
// func Upsert(ctx context.Context, tx *dgo.Txn, data interface{}, options ...MutateOptions) error {
// 	opt := MutateOptions{}
// 	if len(options) > 0 {
// 		opt = options[0]
// 	}

// 	mType, err := newMutateType(data)
// 	if err != nil {
// 		return err
// 	}

// 	cb := func(uniqueErr UniqueError, found, excluded interface{}) interface{} {
// 		log.Println("upserting: ", uniqueErr)
// 		uidIndex, _ := mType.uidIndex()
// 		uid := reflect.ValueOf(found).Elem().Field(uidIndex).String()
// 		reflect.ValueOf(excluded).Elem().Field(uidIndex).SetString(uid)
// 		return excluded
// 	}

// 	return createOrUpdate(ctx, tx, mType, &opt, cb)
// }

// // UpdateOnConflict updates a node
// func UpdateOnConflict(ctx context.Context, tx *dgo.Txn, data interface{}, cb OnConflictCallback, options ...MutateOptions) error {
// 	opt := MutateOptions{}
// 	if len(options) > 0 {
// 		opt = options[0]
// 	}

// 	mType, err := newMutateType(data)
// 	if err != nil {
// 		return err
// 	}

// 	return createOrUpdate(ctx, tx, mType, &opt, cb)
// }
