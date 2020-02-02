package main

import (
	"log"
	"net/http"

	"github.com/dolan-in/dgman"
	"github.com/gin-gonic/gin"
)

type userAPI struct {
	store UserStore
}

func (a *userAPI) Register(c *gin.Context) {
	var user User
	if err := c.Bind(&user); err != nil {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}

	if err := a.store.Create(c, &user); err != nil {
		if uniqueErr, ok := err.(*dgman.UniqueError); ok {
			if uniqueErr.Field == "email" {
				c.AbortWithStatusJSON(http.StatusConflict, gin.H{
					"id":      "emailExists",
					"message": "User with the email already exists",
				})
				return
			}
		}
		log.Println("create user error", err)
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	c.JSON(http.StatusCreated, user.UID)
}

func (a *userAPI) Login(c *gin.Context) {
	var login Login
	if err := c.Bind(&login); err != nil {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}

	valid, err := a.store.CheckPassword(c, &login)
	if err != nil {
		if err == dgman.ErrNodeNotFound {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"id":      "invalidLogin",
				"message": "No user associated with the email",
			})
			return
		}
		log.Println("check password", err)
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	if !valid {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
			"id":      "invalidLogin",
			"message": "No user associated with the email",
		})
		return
	}

	c.AbortWithStatusJSON(http.StatusOK, "ok")
}
