# go-postgres-orm

go-postgresql-orm is a simple postgresql capable ORM library.

## Getting started

### Initializing SQLConnector

```go
var connector PostgreSQLConnector = PostgreSQLConnector{
	Host:        "localhost",
	Port:        "5432",
	User:        "test_orm",
	Password:    "test_orm",
	Database:    "test_orm",
	SSLMode:     "disable", // options: verify-full, verify-ca, disable
	TablePrefix: "orm_",
}
```

### Preparing your models for database

You can tag your models' properties as per below example. It affects how the tables are configured upon creation.

#### Supported tags

You can tag primary key by defining `db_pk`. The primary key column doesn't have to be named "id" but if no `db_pk` is present then `db_column:"id"` is used as a fallback.

| Tag name         | Description                                          |
| ---------------- | ---------------------------------------------------- |
| db_pk            | Marks a field as the primary key                     |
| db_column        | Name of the column in database                       |
| db_column_length | Max length of the column                             |
| db_nullable      | Presence of this tag makes the column nullable       |
| db_unique        | Presence of this tag makes the column unique         |
| db_fk            | Foreign key to another table and field (see example) |
| db_fk_on_delete  | action when the relation is deleted e.g. "SET NULL"  |

_Example:_

```go
type TestModel struct {
	ID          uuid.UUID `db_column:"id" db_pk:""`                    // Primary key (can be any column name)
	StringValue string    `db_column:"string_value" db_column_length:"10"`
	IntValue    int       `db_column:"int_value"`
	UniqueValue string    `db_column:"unique_value" db_unique:""`
}

type TestRelatedModel struct {
	MyPrimaryKey uuid.UUID `db_column:"my_pk" db_pk:""`              // Custom primary key name
	TestModelID  uuid.UUID `db_column:"test_model_id" db_fk:"orm_testmodel(id)" db_nullable:"" db_fk_on_delete:"set null"`
	StringValue  string    `db_column:"string_value"`
}
```

### Connecting to database

You should do this only once when initializing database, the underlying sql library supports connection pooling so there is no need to initialize more than one connectors per database.

_Example:_

```go
	err := connector.Connect()
	if err != nil {
		// handle error
	}
```

### Ping database to verify connection is working

_Example:_

```go
	err := connector.Ping()
	if err != nil {
		// handle error
	}
```

### Automatically create tables from models

go-postgresql-orm creates the tables automatically based on table prefix and model names.

_Example:_

```go

var TABLES = []interface{}{
	&TestModel{},
	&TestRelatedModel{},
}

err := connector.CreateTables(TABLES...)
if err != nil {
    // handle error
}

```

## Core API Methods

The library provides a clean, simplified API with flexible options for context and transactions.

### Options Pattern

All core methods support optional parameters for context and transactions:

- `WithContext(ctx context.Context)` - Add context to operations
- `WithTransaction(tx *sql.Tx)` - Execute within a transaction

### Insert a Model

Insert a single record into the database.

```go
// Basic insert
err := connector.InsertModel(&TestModel{
    ID:          uuid.New(),
    StringValue: "test",
    IntValue:    10,
    UniqueValue: "thisisunique",
})

// With context
ctx := context.Background()
err := connector.InsertModel(&model, WithContext(ctx))

// With transaction
tx, _ := db.Begin()
err := connector.InsertModel(&model, WithTransaction(tx))

// With both context and transaction
err := connector.InsertModel(&model, WithContext(ctx), WithTransaction(tx))
```

### Find First Record

Select a single record by ID or condition. The library automatically detects the primary key field using the `db_pk` tag.

```go
// Find by ID (automatically uses the field marked with db_pk tag)
m := &TestModel{}
err := connector.FindFirst(m, "4d701cf7-e218-4499-8092-7c085118e373")

// Find by condition
condition := Condition{
    Field:    "string_value",
    Operator: "=",
    Value:    "test",
}
err := connector.FindFirst(m, condition)

// With context
err := connector.FindFirst(m, id, WithContext(ctx))
```

### Find Multiple Records

Select multiple records with advanced querying capabilities.

```go
var models []TestModel

// Basic query - get all records
err := connector.FindAll(&models, &DatabaseQuery{})

// With conditions
query := &DatabaseQuery{
    Conditions: []Condition{
        {Field: "int_value", Operator: ">=", Value: 5},
        {Field: "string_value", Operator: "LIKE", Value: "%test%"},
    },
}
err := connector.FindAll(&models, query)

// With pagination
query = &DatabaseQuery{
    Limit:  10,
    Offset: 20,
}
err := connector.FindAll(&models, query)

// With ordering
query = &DatabaseQuery{
    OrderBy:    "string_value",
    Descending: true,
}
err := connector.FindAll(&models, query)

// With search functionality
query = &DatabaseQuery{
    AllowSearch:  true,
    SearchFields: []string{"string_value", "unique_value"},
    SearchTerm:   "search text",
}
err := connector.FindAll(&models, query)

// With joins
query = &DatabaseQuery{
    Joins: []DatabaseJoin{
        {
            Table:         "orm_testrelatedmodel",
            LocalColumn:   "id",
            ForeignColumn: "test_model_id",
            JoinType:      "LEFT",
        },
    },
}
err := connector.FindAll(&models, query)
```

### Update Records

Update records with optional conditions. When no conditions are provided, the library automatically uses the primary key field (marked with `db_pk` tag) for the WHERE clause.

```go
// Update by primary key (automatically detects the field marked with db_pk tag)
model := &TestModel{
    ID:          existingID,
    StringValue: "updated value",
    IntValue:    20,
}
affected, err := connector.UpdateModel(model, nil)

// Update with specific conditions
conditions := []Condition{
    {Field: "string_value", Operator: "=", Value: "old value"},
}
affected, err := connector.UpdateModel(model, conditions)

// With context and transaction
affected, err := connector.UpdateModel(model, conditions, WithContext(ctx), WithTransaction(tx))
```

### Delete Records

Delete records with conditions.

```go
// Delete by conditions
conditions := []Condition{
    {Field: "string_value", Operator: "=", Value: "to_delete"},
}
affected, err := connector.DeleteModel(&TestModel{}, conditions)

// Delete all records (empty conditions)
affected, err := connector.DeleteModel(&TestModel{}, []Condition{})

// With context
affected, err := connector.DeleteModel(&TestModel{}, conditions, WithContext(ctx))
```

## Advanced Features

### Database Query Structure

The `DatabaseQuery` struct provides comprehensive querying capabilities:

```go
type DatabaseQuery struct {
    Conditions      []Condition     // WHERE conditions
    Joins          []DatabaseJoin  // JOIN clauses
    OrderBy        string          // ORDER BY field
    Descending     bool           // DESC/ASC ordering
    Limit          int            // LIMIT clause
    Offset         int            // OFFSET clause
    AllowPagination bool          // Enable pagination parsing from HTTP requests
    AllowSearch    bool          // Enable search parsing from HTTP requests
    SearchTerm     string        // Search term
    SearchFields   []string      // Fields to search in
}
```

### Condition Structure

Define WHERE conditions with flexible operators:

```go
type Condition struct {
    Field    string      // Database column name
    Operator string      // SQL operator (=, !=, >, <, >=, <=, LIKE, IN, etc.)
    Value    interface{} // Value to compare against
}

// Examples
conditions := []Condition{
    {Field: "age", Operator: ">=", Value: 18},
    {Field: "status", Operator: "IN", Value: []string{"active", "pending"}},
    {Field: "name", Operator: "LIKE", Value: "%john%"},
    {Field: "created_at", Operator: ">", Value: time.Now().AddDate(0, 0, -30)},
}
```

### Join Operations

Perform complex queries with joins:

```go
type DatabaseJoin struct {
    Table         string // Table to join
    LocalColumn   string // Local table column
    ForeignColumn string // Foreign table column
    JoinType      string // JOIN type (INNER, LEFT, RIGHT, FULL)
}

// Example: Join users with their orders
query := &DatabaseQuery{
    Joins: []DatabaseJoin{
        {
            Table:         "orm_orders",
            LocalColumn:   "id",
            ForeignColumn: "user_id",
            JoinType:      "LEFT",
        },
    },
}
```

### Custom Queries

For complex operations beyond the standard methods:

```go
// Custom query with results
rows, err := connector.CustomQuery(ctx, nil, "SELECT * FROM custom_view WHERE condition = $1", value)

// Custom mutation (INSERT, UPDATE, DELETE)
result, err := connector.CustomMutate(ctx, tx, "UPDATE table SET field = $1 WHERE id = $2", newValue, id)
```

### HTTP Request Integration

Parse query parameters from HTTP requests for pagination and search:

```go
import "net/http"

func handler(w http.ResponseWriter, r *http.Request) {
    query := &DatabaseQuery{
        AllowPagination: true,
        AllowSearch:     true,
        SearchFields:    []string{"name", "email"},
    }

    // Automatically parses: ?limit=10&offset=20&search=john&order_by=name&desc=true
    ParseQueryParamsFromRequest(r, query)

    var users []User
    err := connector.FindAll(&users, query)
    // ... handle response
}
```

### Transaction Management

Work with database transactions:

```go
// Begin transaction
tx, err := connector.GetConnection().Begin()
if err != nil {
    return err
}
defer tx.Rollback() // Rollback if not committed

// Use transaction with multiple operations
err = connector.InsertModel(&user, WithTransaction(tx))
if err != nil {
    return err
}

err = connector.UpdateModel(&profile, conditions, WithTransaction(tx))
if err != nil {
    return err
}

// Commit transaction
return tx.Commit()
```

### QueryBuilder Utility

Build WHERE clauses and query conditions programmatically:

```go
builder := NewQueryBuilder().
    Where("age", ">", 18).
    Where("status", "=", "active").
    OrderBy("name", true). // true for DESC
    Limit(10)

whereClause, args := builder.Build()
// Produces: " WHERE age > $1 AND status = $2 ORDER BY name DESC LIMIT 10"
// args: [18, "active"]

// Use with custom queries
fullQuery := "SELECT * FROM users" + whereClause
rows, err := connector.CustomQuery(ctx, nil, fullQuery, args...)
```

## Constants

The library defines useful constants:

```go
const (
    DefaultIDField     = "id"           // Default primary key field name
    DBColumnTag        = "db_column"    // Struct tag for column mapping
    DefaultTablePrefix = ""             // Default table prefix
)
```

## Error Handling

All methods return standard Go errors. Handle them appropriately:

```go
err := connector.InsertModel(&model)
if err != nil {
    if strings.Contains(err.Error(), "duplicate key") {
        // Handle duplicate key error
    } else {
        // Handle other errors
        log.Printf("Database error: %v", err)
    }
}
```

## Best Practices

1. **Use transactions** for multiple related operations
2. **Always use context** for timeout and cancellation support
3. **Index your search fields** for better search performance
4. **Use conditions instead of raw SQL** when possible for security
5. **Handle errors appropriately** based on your application needs
6. **Use the QueryBuilder** for complex dynamic queries

## Examples and Tests

For more comprehensive examples, see the test files in the repository. The tests demonstrate:

- Complex query scenarios
- Transaction handling
- HTTP request integration
- Error handling patterns
- Performance considerations
