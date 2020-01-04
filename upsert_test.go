package dgman

// import (
// 	"context"
// 	"log"
// 	"testing"

// 	"github.com/stretchr/testify/assert"
// )

// func TestCreateOrGet(t *testing.T) {
// 	log.SetFlags(log.Lshortfile)
// 	testUnique := []*TestUnique{
// 		&TestUnique{
// 			Name:     "H3h3",
// 			Username: "wildan",
// 			Email:    "wildan2711@gmail.com",
// 			No:       1,
// 		},
// 		&TestUnique{
// 			Name:     "PooDiePie",
// 			Username: "wildansyah",
// 			Email:    "wildansyah2711@gmail.com",
// 			No:       2,
// 		},
// 		&TestUnique{
// 			Name:     "Poopsie",
// 			Username: "wildani",
// 			Email:    "wildani@gmail.com",
// 			No:       3,
// 		},
// 	}

// 	c := newDgraphClient()
// 	if _, err := CreateSchema(c, &TestUnique{}); err != nil {
// 		t.Error(err)
// 	}
// 	defer dropAll(c)

// 	tx := c.NewTxn()

// 	err := Create(context.Background(), tx, &testUnique)
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	if err := tx.Commit(context.Background()); err != nil {
// 		t.Error(err)
// 	}

// 	for _, el := range testUnique {
// 		if el.UID == "" {
// 			t.Error("uid is nil")
// 		}
// 	}

// 	testDuplicate := []*TestUnique{
// 		&TestUnique{
// 			Name:     "H3h3",
// 			Username: "wildanjing",
// 			Email:    "wildan2711@gmail.com",
// 			No:       4,
// 		},
// 		&TestUnique{
// 			Name:     "PooDiePie",
// 			Username: "wildansyah",
// 			Email:    "wildanodol2711@gmail.com",
// 			No:       5,
// 		},
// 		&TestUnique{
// 			Name:     "lalap",
// 			Username: "lalap",
// 			Email:    "lalap@gmail.com",
// 			No:       3,
// 		},
// 	}

// 	tx = c.NewTxn()

// 	if err := CreateOrGet(context.Background(), tx, &testDuplicate); err != nil {
// 		t.Error(err)
// 	}
// 	if err := tx.Commit(context.Background()); err != nil {
// 		t.Error(err)
// 	}

// 	assert.Equal(t, testUnique[0], testDuplicate[0])
// 	assert.Equal(t, testUnique[1], testDuplicate[1])
// 	assert.Equal(t, testUnique[2], testDuplicate[2])
// }

// func TestUpsert(t *testing.T) {
// 	log.SetFlags(log.Lshortfile)
// 	testUnique := []*TestUnique{
// 		&TestUnique{
// 			Name:     "H3h3",
// 			Username: "wildan",
// 			Email:    "wildan2711@gmail.com",
// 			No:       1,
// 		},
// 		&TestUnique{
// 			Name:     "PooDiePie",
// 			Username: "wildansyah",
// 			Email:    "wildansyah2711@gmail.com",
// 			No:       2,
// 		},
// 		&TestUnique{
// 			Name:     "Poopsie",
// 			Username: "wildani",
// 			Email:    "wildani@gmail.com",
// 			No:       3,
// 		},
// 	}

// 	c := newDgraphClient()
// 	if _, err := CreateSchema(c, &TestUnique{}); err != nil {
// 		t.Error(err)
// 	}
// 	defer dropAll(c)

// 	tx := c.NewTxn()

// 	err := Upsert(context.Background(), tx, &testUnique)
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	if err := tx.Commit(context.Background()); err != nil {
// 		t.Error(err)
// 	}

// 	for _, el := range testUnique {
// 		if el.UID == "" {
// 			t.Error("uid is nil")
// 		}
// 	}

// 	testDuplicate := []*TestUnique{
// 		&TestUnique{
// 			Name:     "H3h3",
// 			Username: "wildanjing",
// 			Email:    "wildan2711@gmail.com",
// 			No:       4,
// 		},
// 		&TestUnique{
// 			Name:     "PooDiePie",
// 			Username: "wildansyah",
// 			Email:    "wildanodol2711@gmail.com",
// 			No:       5,
// 		},
// 		&TestUnique{
// 			Name:     "lalap",
// 			Username: "lalap",
// 			Email:    "lalap@gmail.com",
// 			No:       3,
// 		},
// 	}

// 	tx = c.NewTxn()

// 	if err := Upsert(context.Background(), tx, &testDuplicate); err != nil {
// 		t.Error(err)
// 	}
// 	if err := tx.Commit(context.Background()); err != nil {
// 		t.Error(err)
// 	}

// 	assert.Equal(t, testUnique[0].UID, testDuplicate[0].UID)
// 	assert.Equal(t, testUnique[1].UID, testDuplicate[1].UID)
// 	assert.Equal(t, testUnique[2].UID, testDuplicate[2].UID)

// 	tx = c.NewReadOnlyTxn()
// 	for i, tu := range testUnique {
// 		if err := Get(context.Background(), tx, tu).UID(tu.UID).Node(); err != nil {
// 			t.Error(err)
// 		}
// 		assert.Equal(t, testDuplicate[i], tu)
// 	}
// }

// func TestUpsertSingle(t *testing.T) {
// 	log.SetFlags(log.Lshortfile)
// 	testUnique := &TestUnique{
// 		Name:     "H3h3",
// 		Username: "wildan",
// 		Email:    "wildan2711@gmail.com",
// 		No:       1,
// 	}

// 	c := newDgraphClient()
// 	if _, err := CreateSchema(c, &TestUnique{}); err != nil {
// 		t.Error(err)
// 	}
// 	defer dropAll(c)

// 	tx := c.NewTxn()

// 	err := Create(context.Background(), tx, testUnique)
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	if err := tx.Commit(context.Background()); err != nil {
// 		t.Error(err)
// 	}

// 	testDuplicate := &TestUnique{
// 		Name:     "H3h3",
// 		Username: "wildanjing",
// 		Email:    "wildan2711@gmail.com",
// 		No:       4,
// 	}

// 	tx = c.NewTxn()

// 	if err := Upsert(context.Background(), tx, testDuplicate); err != nil {
// 		t.Error(err)
// 	}
// 	if err := tx.Commit(context.Background()); err != nil {
// 		t.Error(err)
// 	}

// 	tx = c.NewReadOnlyTxn()
// 	if err := Get(context.Background(), tx, testUnique).UID(testUnique.UID).Node(); err != nil {
// 		t.Error(err)
// 	}
// 	assert.Equal(t, testDuplicate, testUnique)
// }

// func TestUpdateOnConflict(t *testing.T) {
// 	log.SetFlags(log.Lshortfile)
// 	testUnique := []*TestUnique{
// 		&TestUnique{
// 			Name:     "H3h3",
// 			Username: "wildan",
// 			Email:    "wildan2711@gmail.com",
// 			No:       1,
// 		},
// 		&TestUnique{
// 			Name:     "PooDiePie",
// 			Username: "wildansyah",
// 			Email:    "wildansyah2711@gmail.com",
// 			No:       2,
// 		},
// 		&TestUnique{
// 			Name:     "Poopsie",
// 			Username: "wildani",
// 			Email:    "wildani@gmail.com",
// 			No:       3,
// 		},
// 	}

// 	c := newDgraphClient()
// 	if _, err := CreateSchema(c, &TestUnique{}); err != nil {
// 		t.Error(err)
// 	}
// 	defer dropAll(c)

// 	tx := c.NewTxn()

// 	err := Create(context.Background(), tx, &testUnique)
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	if err := tx.Commit(context.Background()); err != nil {
// 		t.Error(err)
// 	}

// 	for _, el := range testUnique {
// 		if el.UID == "" {
// 			t.Error("uid is nil")
// 		}
// 	}

// 	testDuplicate := []*TestUnique{
// 		&TestUnique{
// 			Name:     "ajiba",
// 			Username: "wildanjing",
// 			Email:    "wildan2711@gmail.com",
// 			No:       4,
// 		},
// 		&TestUnique{
// 			Name:     "PooDiePie",
// 			Username: "wildansyah",
// 			Email:    "wildanodol2711@gmail.com",
// 			No:       5,
// 		},
// 		&TestUnique{
// 			Name:     "lalap",
// 			Username: "lalap",
// 			Email:    "lalap@gmail.com",
// 			No:       3,
// 		},
// 	}

// 	tx = c.NewTxn()

// 	cb := func(uniqueErr UniqueError, found, excluded interface{}) interface{} {
// 		switch uniqueErr.Field {
// 		case "email":
// 			f := found.(*TestUnique)
// 			e := excluded.(*TestUnique)
// 			// just modify the username when email found
// 			f.Username = e.Username
// 			return found
// 		}
// 		return nil
// 	}
// 	if err := UpdateOnConflict(context.Background(), tx, &testDuplicate, cb); err != nil {
// 		t.Error(err)
// 	}
// 	if err := tx.Commit(context.Background()); err != nil {
// 		t.Error(err)
// 	}

// 	assert.Equal(t, testUnique[0].UID, testDuplicate[0].UID)
// 	assert.Equal(t, testUnique[1].UID, testDuplicate[1].UID)
// 	assert.Equal(t, testUnique[2].UID, testDuplicate[2].UID)

// 	tx = c.NewReadOnlyTxn()
// 	for i, tu := range testDuplicate {
// 		if err := Get(context.Background(), tx, tu).UID(tu.UID).Node(); err != nil {
// 			t.Error(err)
// 		}
// 		if i == 0 {
// 			assert.Equal(t, "wildanjing", testDuplicate[i].Username)
// 			assert.Equal(t, "H3h3", testDuplicate[i].Name)
// 			assert.Equal(t, "wildan2711@gmail.com", testDuplicate[i].Email)
// 			assert.Equal(t, 1, testDuplicate[i].No)
// 		} else {
// 			assert.Equal(t, testUnique[i], tu)
// 		}
// 	}
// }
