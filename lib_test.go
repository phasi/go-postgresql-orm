package db

import (
	"testing"

	"github.com/google/uuid"
)

var connector SQLConnector = SQLConnector{}

type TestModel struct {
	ID          uuid.UUID `db_column:"id"`
	StringValue string    `db_column:"string_value" db_unique:"yes" db_column_length:"10"`
	IntValue    int       `db_column:"int_value"`
}
type TestRelatedModel struct {
	ID          uuid.UUID `db_column:"id"`
	TestModelID uuid.UUID `db_column:"test_model_id" db_fk:"testmodel(id)"`
	StringValue string    `db:"string_value"`
}

func TestBuildSimpleQuery(t *testing.T) {

	q, args := connector.buildQuery(&DatabaseQuery{
		Table: "testmodel",
		Model: &TestModel{},
		Condition: []Condition{
			{
				Field:    "id",
				Operator: "=",
				Value:    1,
			},
		},
	})
	if q != "SELECT id,string_value,int_value FROM testmodel WHERE id = $1" {
		t.Errorf("query is not correct")
	}
	// check args $1
	if args[0] != 1 {
		t.Errorf("args are not correct")
	}
}

func TestBuildAdvancedQuery(t *testing.T) {
	q, args := connector.buildAdvancedQuery(&DatabaseQuery{
		Table:           "testmodel",
		Model:           &TestModel{},
		AllowPagination: true,
	}, 10, 15, "int_value", "")
	if q != "SELECT id,string_value,int_value FROM testmodel ORDER BY int_value ASC LIMIT $1 OFFSET $2" {
		t.Errorf("query is not correct")
	}
	// check args $1 and $2
	if args[0] != 10 || args[1] != 15 {
		t.Errorf("args are not correct")
	}
}
