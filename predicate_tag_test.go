/*
 * Copyright (C) 2026 Dolan and Contributors
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
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Film is a test struct where predicate= differs from json tag.
// The json tag uses camelCase (Go convention for JSON APIs)
// but dgraph stores data under snake_case predicate names.
type Film struct {
	UID         string    `json:"uid,omitempty"`
	Title       string    `json:"title,omitempty" dgraph:"index=term"`
	ReleaseDate string    `json:"releaseDate,omitempty" dgraph:"predicate=release_date index=exact"`
	BoxOffice   int       `json:"boxOffice,omitempty" dgraph:"predicate=box_office"`
	DType       []string  `json:"dgraph.type,omitempty"`
}

// Director is a test struct with a dot-prefixed predicate.
type Director struct {
	UID   string   `json:"uid,omitempty"`
	Name  string   `json:"name,omitempty" dgraph:"index=exact"`
	Films []*Film  `json:"films,omitempty" dgraph:"predicate=director.film"`
	DType []string `json:"dgraph.type,omitempty"`
}

// TestMutateBasicPredicateRoundTrip inserts a Film via MutateBasic where predicate= differs
// from the json tag, then reads it back and asserts the field has the correct value.
func TestMutateBasicPredicateRoundTrip(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	_, err := CreateSchema(c, Film{})
	require.NoError(t, err)

	film := Film{
		Title:       "Jurassic Park",
		ReleaseDate: "1993-06-11",
		BoxOffice:   914000000,
	}

	tx := NewTxn(c).SetCommitNow()
	uids, err := tx.MutateBasic(&film)
	require.NoError(t, err)
	assert.NotEmpty(t, uids)
	assert.NotEmpty(t, film.UID)

	// Read it back
	var got Film
	err = NewReadOnlyTxn(c).Get(&got).UID(film.UID).Node()
	require.NoError(t, err)

	assert.Equal(t, "Jurassic Park", got.Title)
	assert.Equal(t, "1993-06-11", got.ReleaseDate, "release_date should round-trip through predicate= tag")
	assert.Equal(t, 914000000, got.BoxOffice, "box_office should round-trip through predicate= tag")
}

// TestMutateUpsertPredicateRoundTrip inserts a Film through the do() path (Mutate/Upsert)
// and verifies the read fix works even when the write side was already correct.
func TestMutateUpsertPredicateRoundTrip(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	// Film with unique field for upsert
	type FilmUnique struct {
		UID         string   `json:"uid,omitempty"`
		Title       string   `json:"title,omitempty" dgraph:"index=term unique"`
		ReleaseDate string   `json:"releaseDate,omitempty" dgraph:"predicate=release_date index=exact"`
		BoxOffice   int      `json:"boxOffice,omitempty" dgraph:"predicate=box_office"`
		DType       []string `json:"dgraph.type,omitempty"`
	}

	_, err := CreateSchema(c, FilmUnique{})
	require.NoError(t, err)

	film := FilmUnique{
		Title:       "The Matrix",
		ReleaseDate: "1999-03-31",
		BoxOffice:   463000000,
	}

	// Use Mutate (do() path) which already uses schema.Predicate for write
	tx := NewTxn(c).SetCommitNow()
	uids, err := tx.Mutate(&film)
	require.NoError(t, err)
	assert.NotEmpty(t, uids)
	assert.NotEmpty(t, film.UID)

	// Read it back
	var got FilmUnique
	err = NewReadOnlyTxn(c).Get(&got).UID(film.UID).Node()
	require.NoError(t, err)

	assert.Equal(t, "The Matrix", got.Title)
	assert.Equal(t, "1999-03-31", got.ReleaseDate, "release_date should round-trip through Mutate/do() path")
	assert.Equal(t, 463000000, got.BoxOffice, "box_office should round-trip through Mutate/do() path")
}

// TestDotPrefixedPredicateRoundTrip verifies that edge predicates with dots
// (like director.film) round-trip correctly through insert and get.
func TestDotPrefixedPredicateRoundTrip(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	_, err := CreateSchema(c, Director{}, Film{})
	require.NoError(t, err)

	// Create films first
	film1 := &Film{
		Title:       "Jurassic Park",
		ReleaseDate: "1993-06-11",
		BoxOffice:   914000000,
	}
	film2 := &Film{
		Title:       "Schindler's List",
		ReleaseDate: "1993-12-15",
		BoxOffice:   322000000,
	}

	tx := NewTxn(c).SetCommitNow()
	_, err = tx.MutateBasic(film1)
	require.NoError(t, err)

	tx = NewTxn(c).SetCommitNow()
	_, err = tx.MutateBasic(film2)
	require.NoError(t, err)

	// Create director with films under director.film predicate
	director := &Director{
		Name:  "Steven Spielberg",
		Films: []*Film{film1, film2},
	}

	tx = NewTxn(c).SetCommitNow()
	_, err = tx.MutateBasic(director)
	require.NoError(t, err)
	assert.NotEmpty(t, director.UID)

	// Read back the director with nested films
	var got Director
	err = NewReadOnlyTxn(c).Get(&got).UID(director.UID).All(1).Node()
	require.NoError(t, err)

	assert.Equal(t, "Steven Spielberg", got.Name)
	require.Len(t, got.Films, 2, "director.film edge should have 2 films")

	// Verify films have correct data including predicate= fields
	filmTitles := make(map[string]string)
	for _, f := range got.Films {
		filmTitles[f.Title] = f.ReleaseDate
	}
	assert.Equal(t, "1993-06-11", filmTitles["Jurassic Park"])
	assert.Equal(t, "1993-12-15", filmTitles["Schindler's List"])
}

// TestQueryFilterByPredicateName inserts films and filters using the dgraph predicate
// name (release_date), asserting correct results come back.
func TestQueryFilterByPredicateName(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	_, err := CreateSchema(c, Film{})
	require.NoError(t, err)

	films := []Film{
		{Title: "Jaws", ReleaseDate: "1975-06-20", BoxOffice: 470000000},
		{Title: "Jurassic Park", ReleaseDate: "1993-06-11", BoxOffice: 914000000},
		{Title: "Saving Private Ryan", ReleaseDate: "1998-07-24", BoxOffice: 482000000},
	}

	for i := range films {
		tx := NewTxn(c).SetCommitNow()
		_, err = tx.MutateBasic(&films[i])
		require.NoError(t, err)
	}

	// Filter using dgraph predicate name: release_date >= "1990-01-01"
	var results []Film
	err = NewReadOnlyTxn(c).Get(&results).Filter(`ge(release_date, "1990-01-01")`).Nodes()
	require.NoError(t, err)

	assert.Len(t, results, 2, "should return 2 films released after 1990")

	releaseDates := make(map[string]bool)
	for _, f := range results {
		releaseDates[f.ReleaseDate] = true
		assert.NotEmpty(t, f.ReleaseDate, "release_date should be deserialized correctly")
		assert.NotZero(t, f.BoxOffice, "box_office should be deserialized correctly")
	}
	assert.True(t, releaseDates["1993-06-11"], "Jurassic Park should be in results")
	assert.True(t, releaseDates["1998-07-24"], "Saving Private Ryan should be in results")
}

// TestPredicateTagSchemaGeneration verifies that predicate= generates the correct
// dgraph schema (predicate names, not json tag names).
func TestPredicateTagSchemaGeneration(t *testing.T) {
	typeSchema := NewTypeSchema()
	typeSchema.Marshal("", &Film{})

	// Verify predicate names are used in schema (not json tag names)
	_, hasReleaseDate := typeSchema.Schema["release_date"]
	assert.True(t, hasReleaseDate, "schema should use predicate name 'release_date'")

	_, hasBoxOffice := typeSchema.Schema["box_office"]
	assert.True(t, hasBoxOffice, "schema should use predicate name 'box_office'")

	// Json tag names should NOT appear in schema
	_, hasReleaseJSON := typeSchema.Schema["releaseDate"]
	assert.False(t, hasReleaseJSON, "schema should NOT use json tag name 'releaseDate'")

	_, hasBoxJSON := typeSchema.Schema["boxOffice"]
	assert.False(t, hasBoxJSON, "schema should NOT use json tag name 'boxOffice'")

	// Director.film should also use the predicate= name
	typeSchema2 := NewTypeSchema()
	typeSchema2.Marshal("", &Director{})

	_, hasDirFilm := typeSchema2.Schema["director.film"]
	assert.True(t, hasDirFilm, "schema should use predicate name 'director.film'")

	_, hasFilmsJSON := typeSchema2.Schema["films"]
	assert.False(t, hasFilmsJSON, "schema should NOT use json tag name 'films'")
}

// TestBuildPredicateToJSONMap verifies the helper function creates the correct mapping.
func TestBuildPredicateToJSONMap(t *testing.T) {
	predMap := buildPredicateToJSONMap(reflect.TypeOf(Film{}))

	assert.Equal(t, "releaseDate", predMap["release_date"])
	assert.Equal(t, "boxOffice", predMap["box_office"])

	// These should NOT be in the map (no predicate override)
	_, hasTitle := predMap["title"]
	assert.False(t, hasTitle, "title has no predicate= override")

	_, hasUID := predMap["uid"]
	assert.False(t, hasUID, "uid should be excluded")
}

// TestRemapPredicateKeys verifies JSON key remapping from predicate to json tag names.
func TestRemapPredicateKeys(t *testing.T) {
	input := []byte(`{"uid":"0x1","title":"Jaws","release_date":"1975-06-20","box_office":470000000,"dgraph.type":["Film"]}`)

	remapped, err := remapPredicateKeys(input, reflect.TypeOf(Film{}))
	require.NoError(t, err)

	var got Film
	err = json.Unmarshal(remapped, &got)
	require.NoError(t, err)

	assert.Equal(t, "0x1", got.UID)
	assert.Equal(t, "Jaws", got.Title)
	assert.Equal(t, "1975-06-20", got.ReleaseDate)
	assert.Equal(t, 470000000, got.BoxOffice)
}

// TestRemapPredicateKeysArray verifies JSON key remapping for arrays.
func TestRemapPredicateKeysArray(t *testing.T) {
	input := []byte(`[{"uid":"0x1","title":"Jaws","release_date":"1975-06-20","box_office":470000000},{"uid":"0x2","title":"ET","release_date":"1982-06-11","box_office":793000000}]`)

	remapped, err := remapPredicateKeys(input, reflect.TypeOf([]Film{}))
	require.NoError(t, err)

	var films []Film
	err = json.Unmarshal(remapped, &films)
	require.NoError(t, err)

	require.Len(t, films, 2)
	assert.Equal(t, "Jaws", films[0].Title)
	assert.Equal(t, "1975-06-20", films[0].ReleaseDate)
	assert.Equal(t, "ET", films[1].Title)
	assert.Equal(t, "1982-06-11", films[1].ReleaseDate)
}

// TestRemapNestedPredicateKeys verifies nested struct remapping.
func TestRemapNestedPredicateKeys(t *testing.T) {
	input := []byte(`{"uid":"0x1","name":"Spielberg","director.film":[{"uid":"0x2","title":"Jaws","release_date":"1975-06-20","box_office":470000000}],"dgraph.type":["Director"]}`)

	remapped, err := remapPredicateKeys(input, reflect.TypeOf(Director{}))
	require.NoError(t, err)

	var got Director
	err = json.Unmarshal(remapped, &got)
	require.NoError(t, err)

	assert.Equal(t, "Spielberg", got.Name)
	require.Len(t, got.Films, 1)
	assert.Equal(t, "Jaws", got.Films[0].Title)
	assert.Equal(t, "1975-06-20", got.Films[0].ReleaseDate)
	assert.Equal(t, 470000000, got.Films[0].BoxOffice)
}

// TestPredicateTagNodesQuery verifies the Nodes() query path with predicate remapping.
func TestPredicateTagNodesQuery(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	_, err := CreateSchema(c, Film{})
	require.NoError(t, err)

	films := []Film{
		{Title: "Film A", ReleaseDate: "2000-01-01", BoxOffice: 100},
		{Title: "Film B", ReleaseDate: "2010-06-15", BoxOffice: 200},
	}

	for i := range films {
		tx := NewTxn(c).SetCommitNow()
		_, err = tx.MutateBasic(&films[i])
		require.NoError(t, err)
	}

	// Query all films
	var results []Film
	err = NewReadOnlyTxn(c).Get(&results).Nodes()
	require.NoError(t, err)

	assert.Len(t, results, 2)
	for _, f := range results {
		assert.NotEmpty(t, f.ReleaseDate, "release_date should be populated")
		assert.NotZero(t, f.BoxOffice, "box_office should be populated")
	}
}

// TestPredicateTagWithTime verifies predicate= works with time.Time fields.
func TestPredicateTagWithTime(t *testing.T) {
	type Event struct {
		UID       string    `json:"uid,omitempty"`
		Name      string    `json:"name,omitempty" dgraph:"index=term"`
		StartTime time.Time `json:"startTime,omitempty" dgraph:"predicate=start_time"`
		DType     []string  `json:"dgraph.type,omitempty"`
	}

	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	_, err := CreateSchema(c, Event{})
	require.NoError(t, err)

	now, _ := time.Parse(time.RFC3339, "2025-01-15T10:30:00Z")
	event := Event{
		Name:      "Conference",
		StartTime: now,
	}

	tx := NewTxn(c).SetCommitNow()
	_, err = tx.MutateBasic(&event)
	require.NoError(t, err)
	assert.NotEmpty(t, event.UID)

	var got Event
	err = NewReadOnlyTxn(c).Get(&got).UID(event.UID).Node()
	require.NoError(t, err)

	assert.Equal(t, "Conference", got.Name)
	assert.Equal(t, now.UTC(), got.StartTime.UTC(), "start_time should round-trip correctly")
}
