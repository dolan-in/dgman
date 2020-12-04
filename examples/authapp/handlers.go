package main

import (
	"encoding/json"
	"log"
	"net/http"
)

type userAPI struct {
	store UserStore
}

func sendJSON(w http.ResponseWriter, code int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	dataJSON, _ := json.Marshal(data)
	w.Write(dataJSON)
}

func (a *userAPI) Register(w http.ResponseWriter, r *http.Request) {
	c := r.Context()

	var user User
	if err := json.NewDecoder(r.Body).Decode(&user); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := a.store.Create(c, &user); err != nil {
		if err == ErrEmailExists {
			sendJSON(w, http.StatusConflict, map[string]string{
				"id":      "emailExists",
				"message": "User with the email already exists",
			})
			return
		}
		log.Println("create user error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	sendJSON(w, http.StatusCreated, user.UID)
}

func (a *userAPI) Login(w http.ResponseWriter, r *http.Request) {
	c := r.Context()

	var login Login
	if err := json.NewDecoder(r.Body).Decode(&login); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	result, err := a.store.CheckPassword(c, &login)
	if err != nil {
		if err == ErrUserNotFound {
			sendJSON(w, http.StatusUnauthorized, map[string]string{
				"id":      "invalidEmail",
				"message": "No user associated with the email",
			})
			return
		}
		log.Println("check password", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if !result.Valid {
		sendJSON(w, http.StatusUnauthorized, map[string]string{
			"id":      "invalidPassword",
			"message": "Invalid password for the email",
		})
		return
	}

	user, err := a.store.Get(c, result.UserID)
	if err != nil {
		log.Println("get user", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	sendJSON(w, http.StatusOK, user)
}
