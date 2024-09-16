package db

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/google/uuid"
)

var connector PostgreSQLConnector = PostgreSQLConnector{
	Host:        "localhost",
	Port:        "5432",
	User:        "test_orm",
	Password:    "test_orm",
	Database:    "test_orm",
	SSLMode:     "disable", // options: verify-full, verify-ca, disable
	TablePrefix: "orm_",
}

var modelId = uuid.New()
var relatedModelId = uuid.New()

func getFakeHttpRequestWithContext() *http.Request {
	req := &http.Request{
		Header: map[string][]string{
			"Content-Type": {"application/json"},
		},

		Method: "GET",
		URL: &url.URL{
			Scheme: "http",
			Host:   "localhost",
			Path:   "/",
		},
	}
	newContext := context.Background()
	return req.WithContext(newContext)
}

func getFakeRequestWithQuery(direction string, limit string, offset string, search string) *http.Request {
	r := getFakeHttpRequestWithContext()
	// Original URL
	originalURL := r.URL.String()

	// Parse the URL
	parsedURL, _ := url.Parse(originalURL)
	// Get the query parameters
	query := parsedURL.Query()
	// Modify the query parameters
	if direction != "" {
		query.Set("direction", direction)
	}
	if limit != "" {
		query.Set("limit", limit)
	}
	if offset != "" {
		query.Set("offset", offset)
	}
	if search != "" {
		query.Set("search", search)
	}
	// Set the modified query back to the URL
	parsedURL.RawQuery = query.Encode()
	r.URL = parsedURL
	return r
}

type TestModel struct {
	ID          uuid.UUID `db_column:"id"`
	StringValue string    `db_column:"string_value" db_column_length:"10"`
	IntValue    int       `db_column:"int_value"`
	UniqueValue string    `db_column:"unique_value" db_unique:"yes"`
}
type TestRelatedModel struct {
	ID          uuid.UUID `db_column:"id"`
	TestModelID uuid.UUID `db_column:"test_model_id" db_fk:"orm_testmodel(id)" db_nullable:"" db_fk_on_delete:"set null"`
	StringValue string    `db:"string_value"`
}

var TABLES = []interface{}{
	&TestModel{},
	&TestRelatedModel{},
}

func TestConnectDatabase(t *testing.T) {
	err := connector.Connect()
	if err != nil {
		t.Errorf("error should be nil but was: %s", err)
	}
}

func TestPingDatabase(t *testing.T) {
	err := connector.Ping()
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
}

func TestCreateTables(t *testing.T) {
	err := connector.CreateTables(TABLES...)
	if err != nil {
		t.Errorf("error should be nil but was: %s", err)
	}
}

func TestInsertModel(t *testing.T) {
	r := getFakeHttpRequestWithContext()
	err := connector.InsertWithContext(r.Context(), &TestModel{
		ID:          modelId,
		StringValue: "test",
		IntValue:    10,
		UniqueValue: "thisisunique",
	})
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
}

func TestSelectModel(t *testing.T) {
	r := getFakeHttpRequestWithContext()
	m := &TestModel{}
	err := connector.FirstWithContext(r.Context(), m, modelId)
	t.Logf("Original model: %v", m)
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
}

func TestInsertRelatedModel(t *testing.T) {
	r := getFakeHttpRequestWithContext()
	err := connector.InsertWithContext(r.Context(), &TestRelatedModel{
		ID:          relatedModelId,
		TestModelID: modelId,
		StringValue: "test related",
	})
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
}

func TestSelectRelatedModel(t *testing.T) {
	r := getFakeHttpRequestWithContext()
	m := &TestRelatedModel{}
	err := connector.FirstWithContext(r.Context(), m, relatedModelId)
	t.Logf("Related model: %v", m)
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
}

func TestUpdateModel(t *testing.T) {
	r := getFakeHttpRequestWithContext()
	affected, err := connector.UpdateWithContext(r.Context(), &TestModel{
		ID:          modelId,
		StringValue: "updated",
		IntValue:    200,
		UniqueValue: "thisisunique",
	})
	if affected == 0 {
		t.Error("update should have succeeded but nothing was changed")
	}
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
}

func TestSelectUpdatedModel(t *testing.T) {
	r := getFakeHttpRequestWithContext()
	m := &TestModel{}
	err := connector.FirstWithContext(r.Context(), m, modelId)
	t.Logf("Updated model: %v", m)
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
}

func TestInsertMoreModels(t *testing.T) {
	r := getFakeHttpRequestWithContext()
	for i := 0; i < 15; i++ {
		model := TestModel{
			ID:          uuid.New(),
			StringValue: fmt.Sprintf("test %d", i),
			IntValue:    i,
			UniqueValue: fmt.Sprintf("thisisunique%d", i),
		}
		err := connector.InsertWithContext(r.Context(), &model)
		if err != nil {
			t.Errorf("error should be nil, but was: %s", err)
		}
	}
}

func TestSelectAllModels(t *testing.T) {
	r := getFakeHttpRequestWithContext()
	models := []TestModel{}
	err := connector.AllWithContext(r.Context(), &models, &DatabaseQuery{
		Model: &TestModel{},
	})
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
	// print models
	for _, model := range models {
		t.Logf("Model: %v", model)
	}
}

func TestSelectAllModelsInDescendingOrder(t *testing.T) {
	r := getFakeHttpRequestWithContext()
	models := []TestModel{}
	err := connector.AllWithContext(r.Context(), &models, &DatabaseQuery{
		Model:      &TestModel{},
		OrderBy:    "int_value",
		Descending: true,
	})
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
	// print models
	for _, model := range models {
		t.Logf("Model: %v", model)
	}
}

func TestSelectAllModelsWithinConditionRange(t *testing.T) {
	r := getFakeHttpRequestWithContext()
	models := []TestModel{}
	err := connector.AllWithContext(r.Context(), &models, &DatabaseQuery{
		Model: &TestModel{},
		Condition: []Condition{
			{
				Field:    "int_value",
				Operator: ">=",
				Value:    5,
			},
			{
				Field:    "int_value",
				Operator: "<=",
				Value:    10,
			},
		},
	})
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
	// print models
	for _, model := range models {
		t.Logf("Model: %v", model)
	}
}

func TestSelectLimitedModels(t *testing.T) {
	r := getFakeHttpRequestWithContext()
	models := []TestModel{}
	err := connector.AllWithContext(r.Context(), &models, &DatabaseQuery{
		Model: &TestModel{},
		Limit: 5,
	})
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
	// print models
	for _, model := range models {
		t.Logf("Model: %v", model)
	}
}

func TestSelectLimitedModelsWithCondition(t *testing.T) {
	r := getFakeHttpRequestWithContext()
	models := []TestModel{}
	err := connector.AllWithContext(r.Context(), &models, &DatabaseQuery{
		Model: &TestModel{},
		Condition: []Condition{
			{
				Field:    "int_value",
				Operator: ">=",
				Value:    5,
			},
			{
				Field:    "int_value",
				Operator: "<=",
				Value:    10,
			},
		},
		Limit: 2,
	})
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
	// print models
	for _, model := range models {
		t.Logf("Model: %v", model)
	}
}

func TestSelectPageOne(t *testing.T) {
	r := getFakeRequestWithQuery("", "5", "", "")
	models := []TestModel{}
	query := &DatabaseQuery{
		Model:           &TestModel{},
		AllowPagination: true,
	}
	ParseQueryParamsFromRequest(r, query)
	err := connector.AllWithContext(r.Context(), &models, query)
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
	// print models
	for _, model := range models {
		t.Logf("Model: %v", model)
	}
}

func TestSelectPageTwo(t *testing.T) {
	r := getFakeRequestWithQuery("", "5", "5", "")
	models := []TestModel{}
	query := &DatabaseQuery{
		Model:           &TestModel{},
		AllowPagination: true,
	}
	ParseQueryParamsFromRequest(r, query)
	err := connector.AllWithContext(r.Context(), &models, query)
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
	// print models
	for _, model := range models {
		t.Logf("Model: %v", model)
	}
}

func TestSelectUsingSearch(t *testing.T) {
	r := getFakeRequestWithQuery("", "", "", "test 2")
	models := []TestModel{}
	query := &DatabaseQuery{
		Model:        &TestModel{},
		AllowSearch:  true,
		SearchFields: []string{"string_value"},
	}
	ParseQueryParamsFromRequest(r, query)
	fmt.Println(query)
	err := connector.AllWithContext(r.Context(), &models, query)
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
	// print models
	for _, model := range models {
		t.Logf("Model: %v", model)
	}
}

func TestDeleteOne(t *testing.T) {
	r := getFakeHttpRequestWithContext()
	err := connector.DeleteByIdWithContext(r.Context(), &TestModel{}, modelId)
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
}

func TestDeleteAllWithContext(t *testing.T) {
	r := getFakeHttpRequestWithContext()
	err := connector.DeleteWithContext(r.Context(), &TestModel{})
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
}

func TestDropTables(t *testing.T) {
	err := connector.DropTables(TABLES...)
	if err != nil {
		t.Errorf("error should be nil but was: %s", err)
	}
}
