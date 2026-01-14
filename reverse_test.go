/*
 * Copyright (C) 2018 Dolan and Contributors
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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Three-level hierarchy for testing managed reverse edges:
// Department -> Course -> Enrollment
//
// Traditional forward edges (what gets stored in Dgraph):
//   Enrollment.in_course -> Course
//   Course.in_department -> Department
//
// Managed reverse edges (defined on parent, creates forward edges on children):
//   Department.Courses -> uses ~in_department
//   Course.Enrollments -> uses ~in_course

// Level 3 (bottom): Enrollment has forward edge to Course
type Enrollment struct {
	UID       string    `json:"uid,omitempty"`
	StudentID string    `json:"student_id,omitempty" dgraph:"index=hash"`
	Grade     string    `json:"grade,omitempty"`
	InCourse  []*Course `json:"in_course,omitempty" dgraph:"reverse"`
	DType     []string  `json:"dgraph.type,omitempty"`
}

// Level 2 (middle): Course has forward edge to Department (single), managed reverse to Enrollments
type Course struct {
	UID          string        `json:"uid,omitempty"`
	Name         string        `json:"course_name,omitempty" dgraph:"index=term,hash"`
	Code         string        `json:"code,omitempty" dgraph:"index=exact"`
	InDepartment *Department   `json:"in_department,omitempty" dgraph:"reverse"` // single edge (course belongs to one department)
	Enrollments  []*Enrollment `json:"~in_course,omitempty" dgraph:"reverse"`    // managed reverse edge
	DType        []string      `json:"dgraph.type,omitempty"`
}

// Level 1 (top): Department has managed reverse edge to Courses
type Department struct {
	UID     string    `json:"uid,omitempty"`
	Name    string    `json:"name,omitempty" dgraph:"index=term,hash unique upsert"` // unique for upsert support
	Budget  int       `json:"budget,omitempty"`
	Courses []*Course `json:"~in_department,omitempty" dgraph:"reverse"` // managed reverse edge
	DType   []string  `json:"dgraph.type,omitempty"`
}

func TestReverseEdgeSchema(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	// Create schema - order matters: bottom-up to ensure forward predicates are defined
	_, err := CreateSchema(c, &Enrollment{}, &Course{}, &Department{})
	require.NoError(t, err)

	// Verify schema was created correctly
	schema, err := GetSchema(c)
	require.NoError(t, err)

	// Forward predicates should exist with @reverse
	assert.Contains(t, schema, "in_course")
	assert.Contains(t, schema, "in_department")
	assert.Contains(t, schema, "@reverse")

	// Managed reverse predicates should NOT be in schema (they're derived)
	// The actual predicate names with ~ should not appear as schema definitions
}

func TestReverseEdgeMutateFromTop(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	_, err := CreateSchema(c, &Enrollment{}, &Course{}, &Department{})
	require.NoError(t, err)

	// Create a department with courses and enrollments from the top level
	dept := &Department{
		Name:   "Computer Science",
		Budget: 1000000,
		Courses: []*Course{
			{
				Name: "Algorithms",
				Code: "CS101",
				Enrollments: []*Enrollment{
					{StudentID: "S001", Grade: "A"},
					{StudentID: "S002", Grade: "B"},
				},
			},
			{
				Name: "Data Structures",
				Code: "CS102",
				Enrollments: []*Enrollment{
					{StudentID: "S001", Grade: "A+"},
					{StudentID: "S003", Grade: "C"},
				},
			},
		},
	}

	tx := NewTxn(c).SetCommitNow()
	_, err = tx.Mutate(dept)
	require.NoError(t, err)

	// Verify UIDs were assigned
	assert.NotEmpty(t, dept.UID, "Department should have UID")
	assert.NotEmpty(t, dept.Courses[0].UID, "Course should have UID")
	assert.NotEmpty(t, dept.Courses[0].Enrollments[0].UID, "Enrollment should have UID")
}

func TestReverseEdgeQueryFromTop(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	_, err := CreateSchema(c, &Enrollment{}, &Course{}, &Department{})
	require.NoError(t, err)

	// Create enrollments first with UIDs
	enrollment1 := &Enrollment{StudentID: "S001", Grade: "A"}
	enrollment2 := &Enrollment{StudentID: "S002", Grade: "B"}

	tx := NewTxn(c).SetCommitNow()
	_, err = tx.Mutate(enrollment1)
	require.NoError(t, err)

	tx = NewTxn(c).SetCommitNow()
	_, err = tx.Mutate(enrollment2)
	require.NoError(t, err)

	// Create course with enrollments (this should create in_course edges on enrollments)
	course := &Course{
		Name:        "Algorithms",
		Code:        "CS101",
		Enrollments: []*Enrollment{enrollment1, enrollment2},
	}

	tx = NewTxn(c).SetCommitNow()
	_, err = tx.Mutate(course)
	require.NoError(t, err)

	// Verify forward edge was created: query enrollment and check in_course
	var gotEnrollment Enrollment
	err = NewReadOnlyTxn(c).Get(&gotEnrollment).UID(enrollment1.UID).All(1).Node()
	require.NoError(t, err)
	require.Len(t, gotEnrollment.InCourse, 1, "Enrollment should have in_course edge to Course")
	assert.Equal(t, course.UID, gotEnrollment.InCourse[0].UID, "in_course should point to the course")

	// Create department with course
	dept := &Department{
		Name:    "Computer Science",
		Budget:  1000000,
		Courses: []*Course{course},
	}

	tx = NewTxn(c).SetCommitNow()
	_, err = tx.Mutate(dept)
	require.NoError(t, err)

	// Query the department and verify reverse edges are populated
	var result Department
	err = NewReadOnlyTxn(c).Get(&result).UID(dept.UID).All(2).Node()
	require.NoError(t, err)

	assert.Equal(t, "Computer Science", result.Name)
	assert.Len(t, result.Courses, 1, "Should have 1 course via reverse edge")
	assert.Equal(t, "Algorithms", result.Courses[0].Name)
	assert.Len(t, result.Courses[0].Enrollments, 2, "Should have 2 enrollments via reverse edge")
}

func TestReverseEdgeQueryFromMiddle(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	_, err := CreateSchema(c, &Enrollment{}, &Course{}, &Department{})
	require.NoError(t, err)

	// Create test data
	dept := &Department{
		Name:   "Mathematics",
		Budget: 500000,
		Courses: []*Course{
			{
				Name: "Calculus",
				Code: "MATH101",
				Enrollments: []*Enrollment{
					{StudentID: "S001", Grade: "A"},
					{StudentID: "S002", Grade: "B+"},
					{StudentID: "S003", Grade: "A-"},
				},
			},
		},
	}

	tx := NewTxn(c).SetCommitNow()
	_, err = tx.Mutate(dept)
	require.NoError(t, err)

	// Query a course directly
	var result Course
	err = NewReadOnlyTxn(c).Get(&result).UID(dept.Courses[0].UID).All(1).Node()
	require.NoError(t, err)

	assert.Equal(t, "Calculus", result.Name)
	assert.Equal(t, "MATH101", result.Code)
	assert.Len(t, result.Enrollments, 3, "Should have 3 enrollments via reverse edge")
}

func TestReverseEdgeGetByUID(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	_, err := CreateSchema(c, &Enrollment{}, &Course{}, &Department{})
	require.NoError(t, err)

	// Create test data
	dept := &Department{
		Name:   "Physics",
		Budget: 750000,
		Courses: []*Course{
			{
				Name: "Quantum Mechanics",
				Code: "PHYS301",
				Enrollments: []*Enrollment{
					{StudentID: "S010", Grade: "B"},
				},
			},
		},
	}

	tx := NewTxn(c).SetCommitNow()
	_, err = tx.Mutate(dept)
	require.NoError(t, err)

	// Get department by UID
	var gotDept Department
	err = NewReadOnlyTxn(c).Get(&gotDept).UID(dept.UID).All(2).Node()
	require.NoError(t, err)

	assert.Equal(t, dept.UID, gotDept.UID)
	assert.Equal(t, "Physics", gotDept.Name)
	assert.Equal(t, 750000, gotDept.Budget)
	require.Len(t, gotDept.Courses, 1)
	assert.Equal(t, "Quantum Mechanics", gotDept.Courses[0].Name)
}

func TestReverseEdgeQueryFilter(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	_, err := CreateSchema(c, &Enrollment{}, &Course{}, &Department{})
	require.NoError(t, err)

	// Create multiple departments
	depts := []*Department{
		{
			Name:   "Computer Science",
			Budget: 1000000,
			Courses: []*Course{
				{Name: "Algorithms", Code: "CS101"},
				{Name: "Networks", Code: "CS201"},
			},
		},
		{
			Name:   "Mathematics",
			Budget: 500000,
			Courses: []*Course{
				{Name: "Calculus", Code: "MATH101"},
			},
		},
	}

	for _, dept := range depts {
		tx := NewTxn(c).SetCommitNow()
		_, err = tx.Mutate(dept)
		require.NoError(t, err)
	}

	// Query departments by filter
	var results []Department
	err = NewReadOnlyTxn(c).Get(&results).Filter(`allofterms(name, "Computer")`).All(1).Nodes()
	require.NoError(t, err)

	require.Len(t, results, 1)
	assert.Equal(t, "Computer Science", results[0].Name)
	assert.Len(t, results[0].Courses, 2, "Should have 2 courses")
}

func TestReverseEdgeDelete(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	_, err := CreateSchema(c, &Enrollment{}, &Course{}, &Department{})
	require.NoError(t, err)

	// Create test data
	dept := &Department{
		Name:   "Biology",
		Budget: 600000,
		Courses: []*Course{
			{
				Name: "Genetics",
				Code: "BIO201",
				Enrollments: []*Enrollment{
					{StudentID: "S020", Grade: "A"},
				},
			},
		},
	}

	tx := NewTxn(c).SetCommitNow()
	_, err = tx.Mutate(dept)
	require.NoError(t, err)

	courseUID := dept.Courses[0].UID

	// Delete the course
	err = NewTxn(c).SetCommitNow().DeleteNode(courseUID)
	require.NoError(t, err)

	// Verify course is deleted
	var gotCourse Course
	err = NewReadOnlyTxn(c).Get(&gotCourse).UID(courseUID).Node()
	assert.Equal(t, ErrNodeNotFound, err, "Course should be deleted")

	// Department should still exist
	var gotDept Department
	err = NewReadOnlyTxn(c).Get(&gotDept).UID(dept.UID).All(1).Node()
	require.NoError(t, err)
	assert.Equal(t, "Biology", gotDept.Name)
}

func TestReverseEdgeUpdate(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	_, err := CreateSchema(c, &Enrollment{}, &Course{}, &Department{})
	require.NoError(t, err)

	// Create test data
	dept := &Department{
		Name:   "Chemistry",
		Budget: 800000,
		Courses: []*Course{
			{
				Name: "Organic Chemistry",
				Code: "CHEM201",
			},
		},
	}

	tx := NewTxn(c).SetCommitNow()
	_, err = tx.Mutate(dept)
	require.NoError(t, err)

	// Update the department
	dept.Budget = 900000
	tx = NewTxn(c).SetCommitNow()
	_, err = tx.Mutate(dept)
	require.NoError(t, err)

	// Verify update
	var gotDept Department
	err = NewReadOnlyTxn(c).Get(&gotDept).UID(dept.UID).All(1).Node()
	require.NoError(t, err)
	assert.Equal(t, 900000, gotDept.Budget)
	assert.Equal(t, "Chemistry", gotDept.Name)
}

func TestReverseEdgeMultipleLevels(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	_, err := CreateSchema(c, &Enrollment{}, &Course{}, &Department{})
	require.NoError(t, err)

	// Create a full 3-level hierarchy
	dept := &Department{
		Name:   "Engineering",
		Budget: 2000000,
		Courses: []*Course{
			{
				Name: "Thermodynamics",
				Code: "ENG201",
				Enrollments: []*Enrollment{
					{StudentID: "E001", Grade: "A"},
					{StudentID: "E002", Grade: "B"},
				},
			},
			{
				Name: "Fluid Mechanics",
				Code: "ENG301",
				Enrollments: []*Enrollment{
					{StudentID: "E001", Grade: "A+"},
					{StudentID: "E003", Grade: "B+"},
					{StudentID: "E004", Grade: "C"},
				},
			},
		},
	}

	tx := NewTxn(c).SetCommitNow()
	_, err = tx.Mutate(dept)
	require.NoError(t, err)

	// Query with full depth to get all levels
	var result Department
	err = NewReadOnlyTxn(c).Get(&result).UID(dept.UID).All(3).Node()
	require.NoError(t, err)

	// Verify all levels
	assert.Equal(t, "Engineering", result.Name)
	require.Len(t, result.Courses, 2, "Should have 2 courses")

	// Find each course and verify enrollments
	for _, course := range result.Courses {
		switch course.Code {
		case "ENG201":
			assert.Equal(t, "Thermodynamics", course.Name)
			assert.Len(t, course.Enrollments, 2, "Thermodynamics should have 2 enrollments")
		case "ENG301":
			assert.Equal(t, "Fluid Mechanics", course.Name)
			assert.Len(t, course.Enrollments, 3, "Fluid Mechanics should have 3 enrollments")
		default:
			t.Errorf("Unexpected course code: %s", course.Code)
		}
	}
}

func TestReverseEdgeAddCourseToExistingDepartment(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	_, err := CreateSchema(c, &Enrollment{}, &Course{}, &Department{})
	require.NoError(t, err)

	// Create a department with one course
	dept := &Department{
		Name:   "Computer Science",
		Budget: 1000000,
		Courses: []*Course{
			{
				Name: "Algorithms",
				Code: "CS101",
				Enrollments: []*Enrollment{
					{StudentID: "S001", Grade: "A"},
				},
			},
		},
	}

	tx := NewTxn(c).SetCommitNow()
	_, err = tx.Mutate(dept)
	require.NoError(t, err)

	// Verify initial state: 1 course
	var result Department
	err = NewReadOnlyTxn(c).Get(&result).UID(dept.UID).All(2).Node()
	require.NoError(t, err)
	require.Len(t, result.Courses, 1, "Should have 1 course initially")
	assert.Equal(t, "Algorithms", result.Courses[0].Name)

	// Now add a second course to the existing department
	newCourse := &Course{
		Name: "Data Structures",
		Code: "CS102",
		Enrollments: []*Enrollment{
			{StudentID: "S002", Grade: "B"},
			{StudentID: "S003", Grade: "A-"},
		},
	}

	// Update department with new course added
	dept.Courses = append(dept.Courses, newCourse)

	tx = NewTxn(c).SetCommitNow()
	_, err = tx.Mutate(dept)
	require.NoError(t, err)

	// Verify: department now has 2 courses
	var updated Department
	err = NewReadOnlyTxn(c).Get(&updated).UID(dept.UID).All(2).Node()
	require.NoError(t, err)

	assert.Equal(t, "Computer Science", updated.Name)
	require.Len(t, updated.Courses, 2, "Should have 2 courses after update")

	// Verify both courses exist with correct enrollments
	courseNames := make(map[string]int)
	for _, course := range updated.Courses {
		courseNames[course.Name] = len(course.Enrollments)
	}

	assert.Equal(t, 1, courseNames["Algorithms"], "Algorithms should have 1 enrollment")
	assert.Equal(t, 2, courseNames["Data Structures"], "Data Structures should have 2 enrollments")

	// Also verify new course has UID assigned
	assert.NotEmpty(t, newCourse.UID, "New course should have UID assigned")
}

func TestReverseEdgeNodes(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	_, err := CreateSchema(c, &Enrollment{}, &Course{}, &Department{})
	require.NoError(t, err)

	// Create multiple departments
	deptNames := []string{"Art", "Music", "Theater"}
	for _, name := range deptNames {
		dept := &Department{
			Name:   name,
			Budget: 100000,
			Courses: []*Course{
				{Name: name + " 101", Code: name[:3] + "101"},
			},
		}
		tx := NewTxn(c).SetCommitNow()
		_, err = tx.Mutate(dept)
		require.NoError(t, err)
	}

	// Query all departments
	var results []Department
	err = NewReadOnlyTxn(c).Get(&results).All(1).Nodes()
	require.NoError(t, err)

	assert.Len(t, results, 3, "Should have 3 departments")

	// Each department should have 1 course via reverse edge
	for _, dept := range results {
		assert.Len(t, dept.Courses, 1, "Each department should have 1 course")
	}
}

func TestReverseEdgeUpsertNewDepartment(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	_, err := CreateSchema(c, &Enrollment{}, &Course{}, &Department{})
	require.NoError(t, err)

	// Upsert a new department with courses
	dept := &Department{
		Name:   "Physics",
		Budget: 800000,
		Courses: []*Course{
			{
				Name: "Mechanics",
				Code: "PHYS101",
				Enrollments: []*Enrollment{
					{StudentID: "P001", Grade: "A"},
				},
			},
		},
	}

	tx := NewTxn(c).SetCommitNow()
	_, err = tx.Upsert(dept, "name")
	require.NoError(t, err)

	assert.NotEmpty(t, dept.UID, "Department should have UID after upsert")
	assert.NotEmpty(t, dept.Courses[0].UID, "Course should have UID after upsert")

	// Verify data was created correctly
	var result Department
	err = NewReadOnlyTxn(c).Get(&result).UID(dept.UID).All(2).Node()
	require.NoError(t, err)

	assert.Equal(t, "Physics", result.Name)
	assert.Equal(t, 800000, result.Budget)
	require.Len(t, result.Courses, 1, "Should have 1 course")
	assert.Equal(t, "Mechanics", result.Courses[0].Name)
	assert.Len(t, result.Courses[0].Enrollments, 1, "Course should have 1 enrollment")
}

func TestReverseEdgeUpsertExistingDepartment(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	_, err := CreateSchema(c, &Enrollment{}, &Course{}, &Department{})
	require.NoError(t, err)

	// Create initial department
	dept := &Department{
		Name:   "Chemistry",
		Budget: 500000,
		Courses: []*Course{
			{Name: "Organic Chemistry", Code: "CHEM201"},
		},
	}

	tx := NewTxn(c).SetCommitNow()
	_, err = tx.Mutate(dept)
	require.NoError(t, err)

	originalUID := dept.UID
	assert.NotEmpty(t, originalUID)

	// Upsert same department name with different budget and additional course
	upsertDept := &Department{
		Name:   "Chemistry", // same name - should match existing
		Budget: 750000,      // updated budget
		Courses: []*Course{
			{Name: "Inorganic Chemistry", Code: "CHEM202"}, // new course
		},
	}

	tx = NewTxn(c).SetCommitNow()
	_, err = tx.Upsert(upsertDept, "name")
	require.NoError(t, err)

	// Should reuse the same UID
	assert.Equal(t, originalUID, upsertDept.UID, "Upsert should reuse existing UID")

	// Verify the department was updated
	var result Department
	err = NewReadOnlyTxn(c).Get(&result).UID(originalUID).All(1).Node()
	require.NoError(t, err)

	assert.Equal(t, "Chemistry", result.Name)
	assert.Equal(t, 750000, result.Budget, "Budget should be updated")

	// Should have both courses (original + new from upsert)
	require.Len(t, result.Courses, 2, "Should have 2 courses after upsert")

	courseNames := make(map[string]bool)
	for _, course := range result.Courses {
		courseNames[course.Name] = true
	}
	assert.True(t, courseNames["Organic Chemistry"], "Should have original course")
	assert.True(t, courseNames["Inorganic Chemistry"], "Should have new course from upsert")
}

func TestReverseEdgeUpsertWithNestedEntities(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	_, err := CreateSchema(c, &Enrollment{}, &Course{}, &Department{})
	require.NoError(t, err)

	// First upsert: create department with course and enrollment
	dept1 := &Department{
		Name:   "Biology",
		Budget: 600000,
		Courses: []*Course{
			{
				Name: "Genetics",
				Code: "BIO301",
				Enrollments: []*Enrollment{
					{StudentID: "B001", Grade: "A"},
					{StudentID: "B002", Grade: "B+"},
				},
			},
		},
	}

	tx := NewTxn(c).SetCommitNow()
	_, err = tx.Upsert(dept1, "name")
	require.NoError(t, err)

	deptUID := dept1.UID
	firstCourseUID := dept1.Courses[0].UID

	// Second upsert: same department, add another course with enrollments
	dept2 := &Department{
		Name:   "Biology", // same name
		Budget: 650000,    // slight budget increase
		Courses: []*Course{
			{
				Name: "Microbiology",
				Code: "BIO302",
				Enrollments: []*Enrollment{
					{StudentID: "B003", Grade: "A-"},
				},
			},
		},
	}

	tx = NewTxn(c).SetCommitNow()
	_, err = tx.Upsert(dept2, "name")
	require.NoError(t, err)

	assert.Equal(t, deptUID, dept2.UID, "Should match existing department")

	// Query full hierarchy
	var result Department
	err = NewReadOnlyTxn(c).Get(&result).UID(deptUID).All(2).Node()
	require.NoError(t, err)

	assert.Equal(t, "Biology", result.Name)
	assert.Equal(t, 650000, result.Budget)
	require.Len(t, result.Courses, 2, "Should have 2 courses after both upserts")

	// Verify enrollments for each course
	totalEnrollments := 0
	for _, course := range result.Courses {
		totalEnrollments += len(course.Enrollments)
		if course.UID == firstCourseUID {
			assert.Equal(t, "Genetics", course.Name)
			assert.Len(t, course.Enrollments, 2, "Genetics should have 2 enrollments")
		} else {
			assert.Equal(t, "Microbiology", course.Name)
			assert.Len(t, course.Enrollments, 1, "Microbiology should have 1 enrollment")
		}
	}
	assert.Equal(t, 3, totalEnrollments, "Total enrollments should be 3")

	// Third upsert: update budget only with empty Courses - should NOT wipe existing courses
	dept3 := &Department{
		Name:   "Biology", // same name
		Budget: 700000,    // budget increase
		// Courses intentionally empty/nil
	}

	tx = NewTxn(c).SetCommitNow()
	_, err = tx.Upsert(dept3, "name")
	require.NoError(t, err)

	assert.Equal(t, deptUID, dept3.UID, "Should still match existing department")

	// Verify courses are preserved
	var result2 Department
	err = NewReadOnlyTxn(c).Get(&result2).UID(deptUID).All(2).Node()
	require.NoError(t, err)

	assert.Equal(t, "Biology", result2.Name)
	assert.Equal(t, 700000, result2.Budget, "Budget should be updated")
	require.Len(t, result2.Courses, 2, "Should still have 2 courses - upsert with empty Courses should not wipe edges")
}

func TestReverseEdgeNavigateFromBottom(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	_, err := CreateSchema(c, &Enrollment{}, &Course{}, &Department{})
	require.NoError(t, err)

	// Create full hierarchy
	dept := &Department{
		Name:   "Mathematics",
		Budget: 900000,
		Courses: []*Course{
			{
				Name: "Calculus",
				Code: "MATH101",
				Enrollments: []*Enrollment{
					{StudentID: "M001", Grade: "A"},
					{StudentID: "M002", Grade: "B+"},
				},
			},
		},
	}

	tx := NewTxn(c).SetCommitNow()
	_, err = tx.Mutate(dept)
	require.NoError(t, err)

	enrollmentUID := dept.Courses[0].Enrollments[0].UID
	courseUID := dept.Courses[0].UID
	deptUID := dept.UID

	// Query starting from Enrollment, navigate up to Course, then to Department
	// Enrollment.InCourse -> Course.InDepartment -> Department
	var enrollment Enrollment
	err = NewReadOnlyTxn(c).Get(&enrollment).UID(enrollmentUID).All(2).Node()
	require.NoError(t, err)

	// Verify we can navigate Enrollment -> Course
	assert.Equal(t, "M001", enrollment.StudentID)
	require.Len(t, enrollment.InCourse, 1, "Enrollment should have in_course edge")
	assert.Equal(t, courseUID, enrollment.InCourse[0].UID)
	assert.Equal(t, "Calculus", enrollment.InCourse[0].Name)

	// Verify we can navigate Course -> Department
	require.NotNil(t, enrollment.InCourse[0].InDepartment, "Course should have in_department edge")
	assert.Equal(t, deptUID, enrollment.InCourse[0].InDepartment.UID)
	assert.Equal(t, "Mathematics", enrollment.InCourse[0].InDepartment.Name)
}

// Person struct for Friend of a Friend graph testing
// Self-referential with bidirectional friends relationship using @reverse
type Person struct {
	UID       string    `json:"uid,omitempty"`
	Name      string    `json:"name,omitempty" dgraph:"index=term,hash"`
	Friends   []*Person `json:"friends,omitempty" dgraph:"reverse"`  // Forward edge with @reverse in schema
	FriendsOf []*Person `json:"~friends,omitempty" dgraph:"reverse"` // Reverse edge for queries
	DType     []string  `json:"dgraph.type,omitempty"`
}

func TestFriendOfFriendSchema(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	_, err := CreateSchema(c, &Person{})
	require.NoError(t, err)

	gotSchema, err := GetSchema(c)
	require.NoError(t, err)

	assert.Contains(t, gotSchema, "friends")
	assert.Contains(t, gotSchema, "@reverse")
	assert.Contains(t, gotSchema, "name")
}

func TestFriendOfFriendBidirectional(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	_, err := CreateSchema(c, &Person{})
	require.NoError(t, err)

	// Create Sally first
	sally := &Person{Name: "Sally"}
	tx := NewTxn(c).SetCommitNow()
	_, err = tx.Mutate(sally)
	require.NoError(t, err)
	require.NotEmpty(t, sally.UID, "Sally should have UID")

	// Create Bob with Sally as a friend (tests self-referential mutation)
	bob := &Person{
		Name:    "Bob",
		Friends: []*Person{sally},
	}
	tx = NewTxn(c).SetCommitNow()
	_, err = tx.Mutate(bob)
	require.NoError(t, err)
	require.NotEmpty(t, bob.UID, "Bob should have UID")
	require.NotEqual(t, "_:", bob.UID[:2], "Bob UID should be resolved, not a blank alias")

	// Query Bob - should have Sally as friend
	var gotBob Person
	err = NewReadOnlyTxn(c).Get(&gotBob).UID(bob.UID).All(3).Node()
	require.NoError(t, err)
	assert.Equal(t, "Bob", gotBob.Name)
	require.Len(t, gotBob.Friends, 1, "Bob should have 1 friend")
	assert.Equal(t, "Sally", gotBob.Friends[0].Name)

	// Query Sally - should have Bob in FriendsOf (reverse edge)
	var gotSally Person
	err = NewReadOnlyTxn(c).Get(&gotSally).UID(sally.UID).All(1).Node()
	require.NoError(t, err)
	assert.Equal(t, "Sally", gotSally.Name)
	require.Len(t, gotSally.FriendsOf, 1, "Sally should have 1 person who friended her")
	assert.Equal(t, "Bob", gotSally.FriendsOf[0].Name)
}

func TestFriendOfFriendChain(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	_, err := CreateSchema(c, &Person{})
	require.NoError(t, err)

	// Create chain: Alice -> Bob -> Carol -> Dave
	dave := &Person{Name: "Dave"}
	tx := NewTxn(c).SetCommitNow()
	_, err = tx.Mutate(dave)
	require.NoError(t, err)

	carol := &Person{Name: "Carol", Friends: []*Person{dave}}
	tx = NewTxn(c).SetCommitNow()
	_, err = tx.Mutate(carol)
	require.NoError(t, err)
	require.NotEmpty(t, carol.UID, "Carol should have UID")

	bob := &Person{Name: "Bob", Friends: []*Person{carol}}
	tx = NewTxn(c).SetCommitNow()
	_, err = tx.Mutate(bob)
	require.NoError(t, err)
	require.NotEmpty(t, bob.UID, "Bob should have UID")

	alice := &Person{Name: "Alice", Friends: []*Person{bob}}
	tx = NewTxn(c).SetCommitNow()
	_, err = tx.Mutate(alice)
	require.NoError(t, err)
	require.NotEmpty(t, alice.UID, "Alice should have UID")

	// Query Alice with depth 3 to traverse: Alice -> Bob -> Carol -> Dave
	var gotAlice Person
	err = NewReadOnlyTxn(c).Get(&gotAlice).UID(alice.UID).All(3).Node()
	require.NoError(t, err)

	// Verify forward chain: Alice -> Bob -> Carol -> Dave
	assert.Equal(t, "Alice", gotAlice.Name)
	require.Len(t, gotAlice.Friends, 1)
	assert.Equal(t, "Bob", gotAlice.Friends[0].Name)
	require.Len(t, gotAlice.Friends[0].Friends, 1)
	assert.Equal(t, "Carol", gotAlice.Friends[0].Friends[0].Name)
	require.Len(t, gotAlice.Friends[0].Friends[0].Friends, 1)
	assert.Equal(t, "Dave", gotAlice.Friends[0].Friends[0].Friends[0].Name)

	// Query Dave and verify reverse edge (one level)
	var gotDave Person
	err = NewReadOnlyTxn(c).Get(&gotDave).UID(dave.UID).All(1).Node()
	require.NoError(t, err)

	assert.Equal(t, "Dave", gotDave.Name)
	require.Len(t, gotDave.FriendsOf, 1, "Dave should have 1 person who friended him")
	assert.Equal(t, "Carol", gotDave.FriendsOf[0].Name)
}

func TestFriendOfFriendMutualFriends(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	_, err := CreateSchema(c, &Person{})
	require.NoError(t, err)

	// Create people
	alice := &Person{Name: "Alice"}
	bob := &Person{Name: "Bob"}
	carol := &Person{Name: "Carol"}

	for _, p := range []*Person{alice, bob, carol} {
		tx := NewTxn(c).SetCommitNow()
		_, err = tx.Mutate(p)
		require.NoError(t, err)
	}

	// Alice and Bob both friend Carol
	alice.Friends = []*Person{carol}
	tx := NewTxn(c).SetCommitNow()
	_, err = tx.Mutate(alice)
	require.NoError(t, err)

	bob.Friends = []*Person{carol}
	tx = NewTxn(c).SetCommitNow()
	_, err = tx.Mutate(bob)
	require.NoError(t, err)

	// Query Carol - should have both Alice and Bob in FriendsOf
	var gotCarol Person
	err = NewReadOnlyTxn(c).Get(&gotCarol).UID(carol.UID).All(1).Node()
	require.NoError(t, err)

	assert.Equal(t, "Carol", gotCarol.Name)
	require.Len(t, gotCarol.FriendsOf, 2, "Carol should have 2 people who friended her")

	friendNames := make(map[string]bool)
	for _, f := range gotCarol.FriendsOf {
		friendNames[f.Name] = true
	}
	assert.True(t, friendNames["Alice"], "Alice should have friended Carol")
	assert.True(t, friendNames["Bob"], "Bob should have friended Carol")
}

func TestFriendOfFriendQueryByName(t *testing.T) {
	c := newDgraphClient()
	dropAll(c)
	defer dropAll(c)

	_, err := CreateSchema(c, &Person{})
	require.NoError(t, err)

	// Build chain: Alice -> Bob -> Carol -> Dave
	dave := &Person{Name: "Dave"}
	tx := NewTxn(c).SetCommitNow()
	_, err = tx.Mutate(dave)
	require.NoError(t, err)

	carol := &Person{Name: "Carol", Friends: []*Person{dave}}
	tx = NewTxn(c).SetCommitNow()
	_, err = tx.Mutate(carol)
	require.NoError(t, err)

	bob := &Person{Name: "Bob", Friends: []*Person{carol}}
	tx = NewTxn(c).SetCommitNow()
	_, err = tx.Mutate(bob)
	require.NoError(t, err)

	alice := &Person{Name: "Alice", Friends: []*Person{bob}}
	tx = NewTxn(c).SetCommitNow()
	_, err = tx.Mutate(alice)
	require.NoError(t, err)

	// Query Carol by name with depth 2 to get nested reverse edges
	var gotCarol Person
	err = NewReadOnlyTxn(c).Get(&gotCarol).Filter(`eq(name, "Carol")`).All(2).Node()
	require.NoError(t, err)

	assert.Equal(t, "Carol", gotCarol.Name)

	// Verify forward path: Carol -> Dave
	require.Len(t, gotCarol.Friends, 1, "Carol should have 1 friend (Dave)")
	assert.Equal(t, "Dave", gotCarol.Friends[0].Name)

	// Verify reverse path: Bob -> Carol (Bob friended Carol)
	require.Len(t, gotCarol.FriendsOf, 1, "Carol should have 1 person who friended her (Bob)")
	assert.Equal(t, "Bob", gotCarol.FriendsOf[0].Name)

	// Verify nested reverse path: Alice -> Bob (Alice friended Bob)
	// This works now that we removed the visited map restriction in writeReverseEdgeBlocks
	require.Len(t, gotCarol.FriendsOf[0].FriendsOf, 1, "Bob should have 1 person who friended him (Alice)")
	assert.Equal(t, "Alice", gotCarol.FriendsOf[0].FriendsOf[0].Name)

	// Query Bob by name and ensure forward/reverse edges are correct
	var gotBob Person
	err = NewReadOnlyTxn(c).Get(&gotBob).Filter(`eq(name, "Bob")`).All(2).Node()
	require.NoError(t, err)
	require.Len(t, gotBob.Friends, 1, "Bob should have 1 friend (Carol)")
	require.Len(t, gotBob.FriendsOf, 1, "Bob should have 1 person who friended him (Alice)")
	require.Equal(t, "Carol", gotBob.Friends[0].Name)
	require.Equal(t, "Alice", gotBob.FriendsOf[0].Name)
	// Alice is at the start of the chain - nobody friended her
	require.Len(t, gotBob.FriendsOf[0].FriendsOf, 0, "Alice should have no one who friended her")
	// Verify forward chain: Bob -> Carol -> Dave
	require.Len(t, gotBob.Friends[0].Friends, 1, "Carol should have 1 friend (Dave)")
	require.Equal(t, "Dave", gotBob.Friends[0].Friends[0].Name)
}
