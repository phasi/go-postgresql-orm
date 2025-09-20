package db

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/google/uuid"
)

func fakeHttpRequest() *http.Request {
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

func fakeHttpRequestWithQueryParams(direction string, limit string, offset string, search string) *http.Request {
	r := fakeHttpRequest()
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

var testUserId = uuid.New()
var testCompanyId = uuid.New()
var testPermissionId = uuid.New()

type TestUser struct {
	ID       uuid.UUID `db_column:"id"`
	Email    string    `db_column:"email" db_unique:"yes"`
	Name     string    `db_column:"name" db_column_length:"30"`
	UserType int       `db_column:"user_type"`
}

type TestUserCompanyPermission struct {
	ID        uuid.UUID `db_column:"id"`
	UserID    uuid.UUID `db_column:"user_id" db_fk:"orm_testuser(id)" db_fk_on_delete:"cascade"`
	CompanyID uuid.UUID `db_column:"company_id" db_fk:"orm_testcompany(id)" db_fk_on_delete:"cascade"`
	Role      string    `db_column:"role"`
}

type TestCompany struct {
	ID          uuid.UUID `db_column:"id"`
	CompanyName string    `db_column:"company_name"`
}

var TABLES = []interface{}{
	&TestUser{},
	&TestCompany{},
	&TestUserCompanyPermission{},
}

var connector PostgreSQLConnector = PostgreSQLConnector{
	Host:        "localhost",
	Port:        "5432",
	User:        "test_orm",
	Password:    "test_orm",
	Database:    "test_orm",
	SSLMode:     "disable", // options: verify-full, verify-ca, disable
	TablePrefix: "orm_",
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

func TestInsertUser(t *testing.T) {
	r := fakeHttpRequest()
	err := connector.InsertWithContext(r.Context(), &TestUser{
		ID:       testUserId,
		Email:    "test@example.com",
		Name:     "Test User",
		UserType: 1,
	})
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
}

func TestSelectUser(t *testing.T) {
	r := fakeHttpRequest()
	m := &TestUser{}
	err := connector.FirstWithContext(r.Context(), m, testUserId)
	t.Logf("Original model: %v", m)
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
}

func TestInsertCompany(t *testing.T) {
	r := fakeHttpRequest()
	err := connector.InsertWithContext(r.Context(), &TestCompany{
		ID:          testCompanyId,
		CompanyName: "Test Company",
	})
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
}

func TestSelectCompany(t *testing.T) {
	r := fakeHttpRequest()
	m := &TestCompany{}
	err := connector.FirstWithContext(r.Context(), m, testCompanyId)
	t.Logf("Original model: %v", m)
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
}

func TestInsertUserCompanyPermission(t *testing.T) {
	r := fakeHttpRequest()
	err := connector.InsertWithContext(r.Context(), &TestUserCompanyPermission{
		ID:        testPermissionId,
		UserID:    testUserId,
		CompanyID: testCompanyId,
		Role:      "admin",
	})
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
}

func TestSelectUserCompanyPermission(t *testing.T) {
	r := fakeHttpRequest()
	m := &TestUserCompanyPermission{}
	err := connector.FirstWithContext(r.Context(), m, testPermissionId)
	t.Logf("Original model: %v", m)
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
}

func TestUpdateUser(t *testing.T) {
	r := fakeHttpRequest()
	affected, err := connector.UpdateWithContext(r.Context(), &TestUser{
		ID:    testUserId,
		Email: "updated@example.com",
		Name:  "Updated User",
	}, nil)
	if affected == 0 {
		t.Error("update should have succeeded but nothing was changed")
	}
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
}

func TestInsertMoreUsers(t *testing.T) {
	r := fakeHttpRequest()
	for i := 0; i < 15; i++ {
		model := TestUser{
			ID:       uuid.New(),
			Email:    fmt.Sprintf("test%d@example.com", i),
			Name:     fmt.Sprintf("Test User %d", i),
			UserType: 1,
		}
		err := connector.InsertWithContext(r.Context(), &model)
		if err != nil {
			t.Errorf("error should be nil, but was: %s", err)
		}
	}
}

func TestSelectAllUsers(t *testing.T) {
	r := fakeHttpRequest()
	models := []TestUser{}
	err := connector.AllWithContext(r.Context(), &models, &DatabaseQuery{})
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
	// print models
	for _, model := range models {
		t.Logf("Model: %v", model)
	}
}

func TestSelectAllUsersInDescendingOrder(t *testing.T) {
	r := fakeHttpRequest()
	models := []TestUser{}
	err := connector.AllWithContext(r.Context(), &models, &DatabaseQuery{
		OrderBy:    "email",
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

func TestSelectUsersWithCondition(t *testing.T) {
	r := fakeHttpRequest()
	models := []TestUser{}
	err := connector.AllWithContext(r.Context(), &models, &DatabaseQuery{
		Conditions: []Condition{
			{
				Field:    "user_type",
				Operator: ">=",
				Value:    1,
			},
			{
				Field:    "user_type",
				Operator: "<=",
				Value:    2,
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

func TestSelectLimitedUsers(t *testing.T) {
	r := fakeHttpRequest()
	models := []TestUser{}
	err := connector.AllWithContext(r.Context(), &models, &DatabaseQuery{
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

func TestSelectLimitedUsersWithCondition(t *testing.T) {
	r := fakeHttpRequest()
	models := []TestUser{}
	err := connector.AllWithContext(r.Context(), &models, &DatabaseQuery{
		Conditions: []Condition{
			{
				Field:    "user_type",
				Operator: "=",
				Value:    1,
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
	r := fakeHttpRequestWithQueryParams("", "5", "", "")
	models := []TestUser{}
	query := &DatabaseQuery{
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
	r := fakeHttpRequestWithQueryParams("", "5", "5", "")
	models := []TestUser{}
	query := &DatabaseQuery{
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

func TestSelectUsersWithSearch(t *testing.T) {
	r := fakeHttpRequestWithQueryParams("", "", "", "test5")
	models := []TestUser{}
	query := &DatabaseQuery{
		AllowSearch:  true,
		SearchFields: []string{"email"},
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
	r := fakeHttpRequest()
	affected, err := connector.DeleteByIdWithContext(r.Context(), &TestUser{}, testUserId)
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
	if affected == 0 {
		t.Error("delete should have succeeded but nothing was changed")
	}
}

func TestDeleteAllWithContext(t *testing.T) {
	r := fakeHttpRequest()
	affected, err := connector.DeleteWithContext(r.Context(), &TestUser{})
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
	if affected == 0 {
		t.Error("delete should have succeeded but nothing was changed")
	}
}

func TestDropTables(t *testing.T) {
	err := connector.DropTables(TABLES...)
	if err != nil {
		t.Errorf("error should be nil but was: %s", err)
	}
}
