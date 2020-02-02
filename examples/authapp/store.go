package main

import (
	"context"
	"time"

	"github.com/dgraph-io/dgo/v2"
	"github.com/dolan-in/dgman"
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
}

type checkPassword struct {
	Valid bool `json:"valid"`
}

type UserStore interface {
	Create(context.Context, *User) error
	CheckPassword(context.Context, *Login) (bool, error)
	Update(context.Context, *User) error
	Get(ctx context.Context, uid string) (*User, error)
}

type userStore struct {
	c *dgo.Dgraph
}

func (s *userStore) Create(ctx context.Context, user *User) error {
	return dgman.NewTxnContext(ctx, s.c).Create(user, true)
}

func (s *userStore) CheckPassword(ctx context.Context, login *Login) (bool, error) {
	result := &checkPassword{}

	tx := dgman.NewReadOnlyTxnContext(ctx, s.c)
	err := tx.Get(&User{}).
		Filter("eq(email, $1)", login.Email).
		Query(`{ valid: checkpwd(password, $1) }`, login.Password).
		Node(result)
	if err != nil {
		return false, err
	}

	return result.Valid, nil
}

func (s *userStore) Update(ctx context.Context, user *User) error {
	return dgman.NewTxnContext(ctx, s.c).Update(user, true)
}

func (s *userStore) Get(ctx context.Context, uid string) (*User, error) {
	user := &User{}
	err := dgman.NewReadOnlyTxnContext(ctx, s.c).
		Get(&User{}).
		UID(uid).
		Node()
	if err != nil {
		return nil, err
	}
	return user, nil
}
