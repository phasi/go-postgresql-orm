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
	ID       uuid.UUID `db_column:"id" db_pk:""`
	Email    string    `db_column:"email" db_unique:"yes"`
	Name     string    `db_column:"name" db_column_length:"30"`
	UserType int       `db_column:"user_type"`
}

type TestUserCompanyPermission struct {
	ID        uuid.UUID `db_column:"id" db_pk:""`
	UserID    uuid.UUID `db_column:"user_id" db_fk:"orm_testuser(id)" db_fk_on_delete:"cascade"`
	CompanyID uuid.UUID `db_column:"company_id" db_fk:"orm_testcompany(id)" db_fk_on_delete:"cascade"`
	Role      string    `db_column:"role"`
}

type TestCompany struct {
	ID          uuid.UUID `db_column:"id" db_pk:""`
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
	err := connector.InsertModel(&TestUser{
		ID:       testUserId,
		Email:    "test@example.com",
		Name:     "Test User",
		UserType: 1,
	}, WithContext(r.Context()))
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
}

func TestSelectUser(t *testing.T) {
	r := fakeHttpRequest()
	m := &TestUser{}
	err := connector.FindFirst(m, testUserId, WithContext(r.Context()))
	t.Logf("Original model: %v", m)
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
}

func TestInsertCompany(t *testing.T) {
	r := fakeHttpRequest()
	err := connector.InsertModel(&TestCompany{
		ID:          testCompanyId,
		CompanyName: "Test Company",
	}, WithContext(r.Context()))
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
}

func TestSelectCompany(t *testing.T) {
	r := fakeHttpRequest()
	m := &TestCompany{}
	err := connector.FindFirst(m, testCompanyId, WithContext(r.Context()))
	t.Logf("Original model: %v", m)
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
}

func TestInsertUserCompanyPermission(t *testing.T) {
	r := fakeHttpRequest()
	err := connector.InsertModel(&TestUserCompanyPermission{
		ID:        testPermissionId,
		UserID:    testUserId,
		CompanyID: testCompanyId,
		Role:      "admin",
	}, WithContext(r.Context()))
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
}

func TestSelectUserCompanyPermission(t *testing.T) {
	r := fakeHttpRequest()
	m := &TestUserCompanyPermission{}
	err := connector.FindFirst(m, testPermissionId, WithContext(r.Context()))
	t.Logf("Original model: %v", m)
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
}

func TestJoinUserWithPermissions(t *testing.T) {
	// Generate new test IDs to avoid conflicts with other tests
	joinTestUserId := uuid.New()
	joinTestCompanyId := uuid.New()
	joinTestPermissionId := uuid.New()

	// First, let's recreate the tables and data for this test
	err := connector.Connect()
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Insert test data
	r := fakeHttpRequest()
	ctx := r.Context()

	// Insert user
	testUser := &TestUser{
		ID:       joinTestUserId,
		Email:    "join@example.com",
		Name:     "Join Test User",
		UserType: 1,
	}
	err = connector.InsertModel(testUser, WithContext(ctx))
	if err != nil {
		t.Fatalf("Failed to insert user: %v", err)
	}

	// Insert company
	testCompany := &TestCompany{
		ID:          joinTestCompanyId,
		CompanyName: "Join Test Company",
	}
	err = connector.InsertModel(testCompany, WithContext(ctx))
	if err != nil {
		t.Fatalf("Failed to insert company: %v", err)
	}

	// Insert user-company permission
	testPermission := &TestUserCompanyPermission{
		ID:        joinTestPermissionId,
		UserID:    joinTestUserId,
		CompanyID: joinTestCompanyId,
		Role:      "admin",
	}
	err = connector.InsertModel(testPermission, WithContext(ctx))
	if err != nil {
		t.Fatalf("Failed to insert permission: %v", err)
	}

	// Test the join functionality
	joinProps := &JoinProps{
		MainTableModel: &TestUser{},
		JoinTableModel: &TestUserCompanyPermission{},
		MainTableCols:  []string{"id", "email", "name"},
		JoinTableCols:  []string{"id", "role"},
		JoinCondition:  "orm_testuser.id = orm_testusercompanypermission.user_id",
		JoinType:       InnerJoin,
		WhereConditions: []Condition{
			{
				Field:    "orm_testuser.id",
				Operator: "=",
				Value:    joinTestUserId,
			},
		},
	}

	results, err := connector.InnerJoinWithContext(ctx, joinProps)
	if err != nil {
		t.Fatalf("Join failed: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("Expected at least one result from join")
	}

	// Verify the results
	firstResult := results[0]
	t.Logf("Join result: %+v", firstResult)

	// Check that we have the expected columns
	expectedColumns := []string{
		"orm_testuser.id", "orm_testuser.email", "orm_testuser.name",
		"orm_testusercompanypermission.id", "orm_testusercompanypermission.role",
	}

	for _, col := range expectedColumns {
		if _, exists := firstResult[col]; !exists {
			t.Errorf("Expected column %s not found in result", col)
		}
	}

	// Verify specific values
	if firstResult["orm_testuser.email"] != "join@example.com" {
		t.Errorf("Expected email 'join@example.com', got %v", firstResult["orm_testuser.email"])
	}

	if firstResult["orm_testusercompanypermission.role"] != "admin" {
		t.Errorf("Expected role 'admin', got %v", firstResult["orm_testusercompanypermission.role"])
	}

	// Verify that UUIDs are properly converted to strings
	userIdStr, ok := firstResult["orm_testuser.id"].(string)
	if !ok {
		t.Errorf("Expected user ID to be string, got %T", firstResult["orm_testuser.id"])
	} else if userIdStr != joinTestUserId.String() {
		t.Errorf("Expected user ID %s, got %s", joinTestUserId.String(), userIdStr)
	}

	// Clean up the test data
	_, err = connector.DeleteModel(&TestUserCompanyPermission{}, []Condition{{Field: "id", Operator: "=", Value: joinTestPermissionId}}, WithContext(ctx))
	if err != nil {
		t.Logf("Warning: Failed to clean up permission: %v", err)
	}

	_, err = connector.DeleteModel(&TestUser{}, []Condition{{Field: "id", Operator: "=", Value: joinTestUserId}}, WithContext(ctx))
	if err != nil {
		t.Logf("Warning: Failed to clean up user: %v", err)
	}

	_, err = connector.DeleteModel(&TestCompany{}, []Condition{{Field: "id", Operator: "=", Value: joinTestCompanyId}}, WithContext(ctx))
	if err != nil {
		t.Logf("Warning: Failed to clean up company: %v", err)
	}
}

func TestLeftJoinUserWithPermissions(t *testing.T) {
	// Test LEFT JOIN: Should return all users, even those without permissions
	userWithPermId := uuid.New()
	userWithoutPermId := uuid.New()
	companyId := uuid.New()
	permissionId := uuid.New()

	err := connector.Connect()
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	r := fakeHttpRequest()
	ctx := r.Context()

	// Insert users - one with permission, one without
	userWithPerm := &TestUser{
		ID:       userWithPermId,
		Email:    "user_with_perm@example.com",
		Name:     "User With Permission",
		UserType: 1,
	}
	err = connector.InsertModel(userWithPerm, WithContext(ctx))
	if err != nil {
		t.Fatalf("Failed to insert user with permission: %v", err)
	}

	userWithoutPerm := &TestUser{
		ID:       userWithoutPermId,
		Email:    "user_without_perm@example.com",
		Name:     "User Without Permission",
		UserType: 1,
	}
	err = connector.InsertModel(userWithoutPerm, WithContext(ctx))
	if err != nil {
		t.Fatalf("Failed to insert user without permission: %v", err)
	}

	// Insert company
	testCompany := &TestCompany{
		ID:          companyId,
		CompanyName: "Test Company",
	}
	err = connector.InsertModel(testCompany, WithContext(ctx))
	if err != nil {
		t.Fatalf("Failed to insert company: %v", err)
	}

	// Insert permission for only one user
	testPermission := &TestUserCompanyPermission{
		ID:        permissionId,
		UserID:    userWithPermId,
		CompanyID: companyId,
		Role:      "admin",
	}
	err = connector.InsertModel(testPermission, WithContext(ctx))
	if err != nil {
		t.Fatalf("Failed to insert permission: %v", err)
	}

	// Test LEFT JOIN - should return both users
	joinProps := &JoinProps{
		MainTableModel: &TestUser{},
		JoinTableModel: &TestUserCompanyPermission{},
		MainTableCols:  []string{"id", "email", "name"},
		JoinTableCols:  []string{"id", "role"},
		JoinCondition:  "orm_testuser.id = orm_testusercompanypermission.user_id",
		WhereConditions: []Condition{
			{
				Field:    "orm_testuser.email",
				Operator: "LIKE",
				Value:    "user_%_perm@example.com",
			},
		},
	}

	results, err := connector.LeftJoinWithContext(ctx, joinProps)
	if err != nil {
		t.Fatalf("Left join failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("Expected 2 results from left join, got %d", len(results))
	}

	// Verify we have both users (one with permission data, one with NULLs)
	hasUserWithPerm := false
	hasUserWithoutPerm := false

	for _, result := range results {
		userEmail := result["orm_testuser.email"]
		if userEmail == "user_with_perm@example.com" {
			hasUserWithPerm = true
			// Should have permission data
			if result["orm_testusercompanypermission.role"] != "admin" {
				t.Errorf("Expected admin role for user with permission, got %v", result["orm_testusercompanypermission.role"])
			}
		} else if userEmail == "user_without_perm@example.com" {
			hasUserWithoutPerm = true
			// Should have NULL/nil permission data
			if result["orm_testusercompanypermission.role"] != nil {
				t.Errorf("Expected nil role for user without permission, got %v", result["orm_testusercompanypermission.role"])
			}
		}
	}

	if !hasUserWithPerm || !hasUserWithoutPerm {
		t.Error("LEFT JOIN should return both users (with and without permissions)")
	}

	// Clean up
	connector.DeleteModel(&TestUserCompanyPermission{}, []Condition{{Field: "id", Operator: "=", Value: permissionId}}, WithContext(ctx))
	connector.DeleteModel(&TestUser{}, []Condition{{Field: "id", Operator: "=", Value: userWithPermId}}, WithContext(ctx))
	connector.DeleteModel(&TestUser{}, []Condition{{Field: "id", Operator: "=", Value: userWithoutPermId}}, WithContext(ctx))
	connector.DeleteModel(&TestCompany{}, []Condition{{Field: "id", Operator: "=", Value: companyId}}, WithContext(ctx))
}

func TestRightJoinUserWithPermissions(t *testing.T) {
	// Test RIGHT JOIN: Should return all permissions, with their users if they exist
	user1Id := uuid.New()
	user2Id := uuid.New()
	companyId := uuid.New()
	perm1Id := uuid.New()
	perm2Id := uuid.New()

	err := connector.Connect()
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	r := fakeHttpRequest()
	ctx := r.Context()

	// Clean up any existing data with the same IDs first
	connector.DeleteModel(&TestUserCompanyPermission{}, []Condition{{Field: "id", Operator: "=", Value: perm1Id}}, WithContext(ctx))
	connector.DeleteModel(&TestUserCompanyPermission{}, []Condition{{Field: "id", Operator: "=", Value: perm2Id}}, WithContext(ctx))
	connector.DeleteModel(&TestUser{}, []Condition{{Field: "id", Operator: "=", Value: user1Id}}, WithContext(ctx))
	connector.DeleteModel(&TestUser{}, []Condition{{Field: "id", Operator: "=", Value: user2Id}}, WithContext(ctx))
	connector.DeleteModel(&TestCompany{}, []Condition{{Field: "id", Operator: "=", Value: companyId}}, WithContext(ctx))

	// Insert two users
	user1 := &TestUser{
		ID:       user1Id,
		Email:    fmt.Sprintf("user1_right_%s@example.com", user1Id.String()[:8]),
		Name:     "User 1",
		UserType: 1,
	}
	err = connector.InsertModel(user1, WithContext(ctx))
	if err != nil {
		t.Fatalf("Failed to insert user1: %v", err)
	}

	user2 := &TestUser{
		ID:       user2Id,
		Email:    fmt.Sprintf("user2_right_%s@example.com", user2Id.String()[:8]),
		Name:     "User 2",
		UserType: 1,
	}
	err = connector.InsertModel(user2, WithContext(ctx))
	if err != nil {
		t.Fatalf("Failed to insert user2: %v", err)
	}

	// Insert company
	testCompany := &TestCompany{
		ID:          companyId,
		CompanyName: "Right Join Test Company",
	}
	err = connector.InsertModel(testCompany, WithContext(ctx))
	if err != nil {
		t.Fatalf("Failed to insert company: %v", err)
	}

	// Insert two permissions (for both users)
	perm1 := &TestUserCompanyPermission{
		ID:        perm1Id,
		UserID:    user1Id,
		CompanyID: companyId,
		Role:      "admin",
	}
	err = connector.InsertModel(perm1, WithContext(ctx))
	if err != nil {
		t.Fatalf("Failed to insert permission 1: %v", err)
	}

	perm2 := &TestUserCompanyPermission{
		ID:        perm2Id,
		UserID:    user2Id,
		CompanyID: companyId,
		Role:      "viewer",
	}
	err = connector.InsertModel(perm2, WithContext(ctx))
	if err != nil {
		t.Fatalf("Failed to insert permission 2: %v", err)
	}

	// Test RIGHT JOIN - should return all permissions with their users
	joinProps := &JoinProps{
		MainTableModel: &TestUser{},
		JoinTableModel: &TestUserCompanyPermission{},
		MainTableCols:  []string{"id", "email", "name"},
		JoinTableCols:  []string{"id", "role"},
		JoinCondition:  "orm_testuser.id = orm_testusercompanypermission.user_id",
		JoinType:       RightJoin,
		WhereConditions: []Condition{
			{
				Field:    "orm_testuser.email",
				Operator: "LIKE",
				Value:    "%_right_%@example.com",
			},
		},
	}

	results, err := connector.RightJoinWithContext(ctx, joinProps)
	if err != nil {
		t.Fatalf("Right join failed: %v", err)
	}

	if len(results) != 2 {
		t.Logf("Found %d results:", len(results))
		for i, result := range results {
			t.Logf("  %d: user=%v, role=%v", i+1, result["orm_testuser.email"], result["orm_testusercompanypermission.role"])
		}
		t.Fatalf("Expected 2 results from right join, got %d", len(results))
	}

	// Verify we have both permissions with their respective users
	foundAdminPerm := false
	foundViewerPerm := false

	for _, result := range results {
		permRole := result["orm_testusercompanypermission.role"]
		userEmail := result["orm_testuser.email"]

		if permRole == "admin" {
			foundAdminPerm = true
			expectedEmail := fmt.Sprintf("user1_right_%s@example.com", user1Id.String()[:8])
			if userEmail != expectedEmail {
				t.Errorf("Expected user email %s for admin permission, got %v", expectedEmail, userEmail)
			}
		} else if permRole == "viewer" {
			foundViewerPerm = true
			expectedEmail := fmt.Sprintf("user2_right_%s@example.com", user2Id.String()[:8])
			if userEmail != expectedEmail {
				t.Errorf("Expected user email %s for viewer permission, got %v", expectedEmail, userEmail)
			}
		}
	}

	if !foundAdminPerm {
		t.Error("RIGHT JOIN should include admin permission")
	}
	if !foundViewerPerm {
		t.Error("RIGHT JOIN should include viewer permission")
	}

	// Clean up
	connector.DeleteModel(&TestUserCompanyPermission{}, []Condition{{Field: "id", Operator: "=", Value: perm1Id}}, WithContext(ctx))
	connector.DeleteModel(&TestUserCompanyPermission{}, []Condition{{Field: "id", Operator: "=", Value: perm2Id}}, WithContext(ctx))
	connector.DeleteModel(&TestUser{}, []Condition{{Field: "id", Operator: "=", Value: user1Id}}, WithContext(ctx))
	connector.DeleteModel(&TestUser{}, []Condition{{Field: "id", Operator: "=", Value: user2Id}}, WithContext(ctx))
	connector.DeleteModel(&TestCompany{}, []Condition{{Field: "id", Operator: "=", Value: companyId}}, WithContext(ctx))
}

func TestFullJoinUserWithPermissions(t *testing.T) {
	// Test FULL OUTER JOIN: Should return all users AND all permissions, matched where possible
	// We'll use a simpler approach that works within FK constraints
	userWithPermId := uuid.New()
	userWithoutPermId := uuid.New()
	companyId := uuid.New()
	permissionId := uuid.New()

	err := connector.Connect()
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	r := fakeHttpRequest()
	ctx := r.Context()

	// Insert users
	userWithPerm := &TestUser{
		ID:       userWithPermId,
		Email:    fmt.Sprintf("user_with_perm_full_%s@example.com", userWithPermId.String()[:8]),
		Name:     "User With Permission",
		UserType: 1,
	}
	err = connector.InsertModel(userWithPerm, WithContext(ctx))
	if err != nil {
		t.Fatalf("Failed to insert user with permission: %v", err)
	}

	userWithoutPerm := &TestUser{
		ID:       userWithoutPermId,
		Email:    fmt.Sprintf("user_without_perm_full_%s@example.com", userWithoutPermId.String()[:8]),
		Name:     "User Without Permission",
		UserType: 1,
	}
	err = connector.InsertModel(userWithoutPerm, WithContext(ctx))
	if err != nil {
		t.Fatalf("Failed to insert user without permission: %v", err)
	}

	// Insert company
	testCompany := &TestCompany{
		ID:          companyId,
		CompanyName: "Full Join Test Company",
	}
	err = connector.InsertModel(testCompany, WithContext(ctx))
	if err != nil {
		t.Fatalf("Failed to insert company: %v", err)
	}

	// Insert permission for only one user
	testPermission := &TestUserCompanyPermission{
		ID:        permissionId,
		UserID:    userWithPermId,
		CompanyID: companyId,
		Role:      "admin",
	}
	err = connector.InsertModel(testPermission, WithContext(ctx))
	if err != nil {
		t.Fatalf("Failed to insert permission: %v", err)
	}

	// Test FULL OUTER JOIN - should return all users and all permissions
	joinProps := &JoinProps{
		MainTableModel: &TestUser{},
		JoinTableModel: &TestUserCompanyPermission{},
		MainTableCols:  []string{"id", "email", "name"},
		JoinTableCols:  []string{"id", "role"},
		JoinCondition:  "orm_testuser.id = orm_testusercompanypermission.user_id",
		JoinType:       FullJoin,
		WhereConditions: []Condition{
			{
				Field:    "orm_testuser.email",
				Operator: "LIKE",
				Value:    "%_full_%@example.com",
			},
		},
	}

	results, err := connector.FullJoinWithContext(ctx, joinProps)
	if err != nil {
		t.Fatalf("Full join failed: %v", err)
	}

	// Should return at least 2 rows:
	// 1. User with permission + permission data
	// 2. User without permission + NULL permission data
	if len(results) < 2 {
		t.Fatalf("Expected at least 2 results from full join, got %d", len(results))
	}

	t.Logf("Full join returned %d results", len(results))

	// Verify we have both scenarios
	hasUserWithPerm := false
	hasUserWithoutPerm := false

	for i, result := range results {
		userEmail := result["orm_testuser.email"]
		permRole := result["orm_testusercompanypermission.role"]

		t.Logf("Result %d: user=%v, role=%v", i+1, userEmail, permRole)

		if userEmail == fmt.Sprintf("user_with_perm_full_%s@example.com", userWithPermId.String()[:8]) {
			hasUserWithPerm = true
			if permRole != "admin" {
				t.Errorf("Expected admin role for user with permission, got %v", permRole)
			}
		} else if userEmail == fmt.Sprintf("user_without_perm_full_%s@example.com", userWithoutPermId.String()[:8]) {
			hasUserWithoutPerm = true
			if permRole != nil {
				t.Errorf("Expected nil role for user without permission, got %v", permRole)
			}
		}
	}

	if !hasUserWithPerm {
		t.Error("FULL OUTER JOIN should include user with permission")
	}
	if !hasUserWithoutPerm {
		t.Error("FULL OUTER JOIN should include user without permission")
	}

	// Clean up
	connector.DeleteModel(&TestUserCompanyPermission{}, []Condition{{Field: "id", Operator: "=", Value: permissionId}}, WithContext(ctx))
	connector.DeleteModel(&TestUser{}, []Condition{{Field: "id", Operator: "=", Value: userWithPermId}}, WithContext(ctx))
	connector.DeleteModel(&TestUser{}, []Condition{{Field: "id", Operator: "=", Value: userWithoutPermId}}, WithContext(ctx))
	connector.DeleteModel(&TestCompany{}, []Condition{{Field: "id", Operator: "=", Value: companyId}}, WithContext(ctx))
}

func TestUpdateUser(t *testing.T) {
	r := fakeHttpRequest()
	affected, err := connector.UpdateModel(&TestUser{
		ID:    testUserId,
		Email: "updated@example.com",
		Name:  "Updated User",
	}, nil, WithContext(r.Context()))
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
		err := connector.InsertModel(&model, WithContext(r.Context()))
		if err != nil {
			t.Errorf("error should be nil, but was: %s", err)
		}
	}
}

func TestSelectAllUsers(t *testing.T) {
	r := fakeHttpRequest()
	models := []TestUser{}
	err := connector.FindAll(&models, &DatabaseQuery{}, WithContext(r.Context()))
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
	err := connector.FindAll(&models, &DatabaseQuery{
		OrderBy:    "email",
		Descending: true,
	}, WithContext(r.Context()))
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
	err := connector.FindAll(&models, &DatabaseQuery{
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
	}, WithContext(r.Context()))
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
	err := connector.FindAll(&models, &DatabaseQuery{
		Limit: 5,
	}, WithContext(r.Context()))
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
	err := connector.FindAll(&models, &DatabaseQuery{
		Conditions: []Condition{
			{
				Field:    "user_type",
				Operator: "=",
				Value:    1,
			},
		},
		Limit: 2,
	}, WithContext(r.Context()))
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
	err := connector.FindAll(&models, query, WithContext(r.Context()))
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
	err := connector.FindAll(&models, query, WithContext(r.Context()))
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
	err := connector.FindAll(&models, query, WithContext(r.Context()))
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
	testUser := &TestUser{}
	pkField := getPrimaryKeyField(testUser)
	condition := Condition{
		Field:    pkField,
		Operator: "=",
		Value:    testUserId,
	}
	affected, err := connector.DeleteModel(testUser, []Condition{condition}, WithContext(r.Context()))
	if err != nil {
		t.Errorf("error should be nil, but was: %s", err)
	}
	if affected == 0 {
		t.Error("delete should have succeeded but nothing was changed")
	}
}

func TestDeleteAllWithContext(t *testing.T) {
	r := fakeHttpRequest()
	affected, err := connector.DeleteModel(&TestUser{}, []Condition{}, WithContext(r.Context()))
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
