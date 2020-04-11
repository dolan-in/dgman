package main

import (
	"context"
	"errors"
	"time"

	"github.com/dgraph-io/dgo/v200"
	"github.com/dolan-in/dgman"
)

var (
	ErrEmailExists  = errors.New("email already exists")
	ErrUserNotFound = errors.New("user not found")
)

type Login struct {
	Email    string
	Password string
}

type User struct {
	UID      string     `json:"uid,omitempty"`
	Fullname string     `json:"fullname,omitempty" dgraph:"index=term"`
	Email    string     `json:"email,omitempty" dgraph:"index=exact unique"`
	Password string     `json:"password,omitempty" dgraph:"type=password"`
	Dob      *time.Time `json:"dob,omitempty"`
	DType    []string   `json:"dgraph.type,omitempty"`
}

type CheckPassword struct {
	UserID string `json:"uid"`
	Valid  bool   `json:"valid"`
}

type UserStore interface {
	Create(context.Context, *User) error
	CheckPassword(context.Context, *Login) (*CheckPassword, error)
	Get(ctx context.Context, uid string) (*User, error)
}

type userStore struct {
	c *dgo.Dgraph
}

func (s *userStore) Create(ctx context.Context, user *User) error {
	err := dgman.NewTxnContext(ctx, s.c).Create(user, true)
	if err != nil {
		if uniqueErr, ok := err.(*dgman.UniqueError); ok {
			if uniqueErr.Field == "email" {
				return ErrEmailExists
			}
		}
		return err
	}
	return nil
}

func (s *userStore) CheckPassword(ctx context.Context, login *Login) (*CheckPassword, error) {
	result := &CheckPassword{}

	tx := dgman.NewReadOnlyTxnContext(ctx, s.c)
	err := tx.Get(&User{}).
		Filter("eq(email, $1)", login.Email).
		Query(`{ 
			uid
			valid: checkpwd(password, $1) 
		}`, login.Password).
		Node(result)
	if err != nil {
		if err == dgman.ErrNodeNotFound {
			return nil, ErrUserNotFound
		}
	}

	return result, nil
}

func (s *userStore) Get(ctx context.Context, uid string) (*User, error) {
	user := &User{}
	err := dgman.NewReadOnlyTxnContext(ctx, s.c).
		Get(user).
		UID(uid).
		Node()
	if err != nil {
		if err == dgman.ErrNodeNotFound {
			return nil, ErrUserNotFound
		}
	}
	return user, nil
}
