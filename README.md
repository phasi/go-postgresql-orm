# go-postgresql-orm

go-postgresql-orm is a simple postgresql capable ORM library.

## Installation

### Prerequisites

- Go 1.16 or later
- PostgreSQL database server

### Install the library

Warning! Do not use versions prior to v1.1.1

```bash
go get github.com/lib/pq
go get github.com/phasi/go-postgresql-orm@v1.1.1
```

### Import in your Go project

```go
import (
    "github.com/google/uuid"
    _ "github.com/lib/pq"
    gpo "github.com/phasi/go-postgresql-orm"
)
```

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

You can tag your models' properties using the unified `gpo` tag system. It affects how the tables are configured upon creation.

#### GPO Tag System

The `gpo` tag uses a comma-separated format: `gpo:"column_name,option1,option2,..."` where the first part is always the column name, followed by optional modifiers.

| Option                 | Description                                     | Example                             |
| ---------------------- | ----------------------------------------------- | ----------------------------------- |
| `pk`                   | Marks a field as the primary key                | `gpo:"id,pk"`                       |
| `unique`               | Makes the column unique                         | `gpo:"email,unique"`                |
| `nullable`             | Makes the column nullable (default is NOT NULL) | `gpo:"description,nullable"`        |
| `length(n)`            | Sets maximum length for string columns          | `gpo:"name,length(50)"`             |
| `fk(table:col)`        | Foreign key to another table and column         | `gpo:"user_id,fk(user:id)"`         |
| `fk(table:col,action)` | Foreign key with ON DELETE action               | `gpo:"user_id,fk(user:id,cascade)"` |

**Foreign Key Notes:**

- Table names in foreign keys should NOT include the table prefix
- The ORM automatically adds the configured table prefix
- Supported ON DELETE actions: `cascade`, `set null`, `restrict`, `no action`, `set default`

_Example:_

```go
type User struct {
	ID          uuid.UUID `gpo:"id,pk"`                          // Primary key
	Email       string    `gpo:"email,unique"`                   // Unique email
	Name        string    `gpo:"name,length(50)"`               // Max 50 characters
	Description string    `gpo:"description,nullable"`           // Nullable field
	Age         int       `gpo:"age"`                           // Regular integer field
}

type UserProfile struct {
	ID     uuid.UUID `gpo:"id,pk"`                             // Primary key
	UserID uuid.UUID `gpo:"user_id,fk(user:id,cascade)"`      // Foreign key with cascade delete
	Bio    string    `gpo:"bio,length(500),nullable"`          // Multiple options
}

type Post struct {
	ID       uuid.UUID `gpo:"id,pk"`                           // Primary key
	AuthorID uuid.UUID `gpo:"author_id,fk(user:id,set null)"` // FK with SET NULL on delete
	Title    string    `gpo:"title,length(200)"`               // Required title
	Content  string    `gpo:"content"`                         // TEXT field (no length limit)
	Slug     string    `gpo:"slug,unique,length(100)"`         // Unique slug with length limit
}
```

**Key Features:**

- ✅ **Custom primary keys**: Any field can be the primary key with `pk` option
- ✅ **Automatic table prefixes**: Foreign keys automatically get the configured table prefix
- ✅ **Multiple constraints**: Combine `unique`, `nullable`, `length()` in any order
- ✅ **Smart defaults**: If no `pk` field is defined, an `id UUID PRIMARY KEY` is automatically created

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
	&User{},
	&UserProfile{},
	&Post{},
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
err := connector.InsertModel(&User{
    ID:          uuid.New(),
    Email:       "user@example.com",
    Name:        "John Doe",
    Description: "A sample user",
    Age:         30,
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

Select a single record by ID or condition. The library automatically detects the primary key field using the `pk` option in the `gpo` tag.

```go
// Find by ID (automatically uses the field marked with pk tag)
user := &User{}
err := connector.FindFirst(user, "4d701cf7-e218-4499-8092-7c085118e373")

// Find by condition
condition := Condition{
    Field:    "email",
    Operator: "=",
    Value:    "user@example.com",
}
err := connector.FindFirst(m, condition)

// With context
err := connector.FindFirst(m, id, WithContext(ctx))
```

### Find Multiple Records

Select multiple records with advanced querying capabilities.

```go
var users []User

// Basic query - get all records
err := connector.FindAll(&users, &DatabaseQuery{})

// With conditions
query := &DatabaseQuery{
    Conditions: []Condition{
        {Field: "age", Operator: ">=", Value: 18},
        {Field: "name", Operator: "LIKE", Value: "%John%"},
    },
}
err := connector.FindAll(&users, query)

// With pagination
query = &DatabaseQuery{
    Limit:  10,
    Offset: 20,
}
err := connector.FindAll(&users, query)

// With ordering
query = &DatabaseQuery{
    OrderBy:    "name",
    Descending: true,
}
err := connector.FindAll(&users, query)

// With search functionality
query = &DatabaseQuery{
    AllowSearch:  true,
    SearchFields: []string{"name", "email"},
    SearchTerm:   "search text",
}
err := connector.FindAll(&users, query)

```

### Update Records

Update records with optional conditions. When no conditions are provided, the library automatically uses the primary key field (marked with `pk` option in the `gpo` tag) for the WHERE clause.

```go
// Update by primary key (automatically detects the field marked with pk tag)
user := &User{
    ID:          existingID,
    Email:       "updated@example.com",
    Name:        "Updated Name",
    Age:         25,
}
affected, err := connector.UpdateModel(user, nil)

// Update with specific conditions
conditions := []Condition{
    {Field: "name", Operator: "=", Value: "Old Name"},
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
    {Field: "email", Operator: "=", Value: "delete@example.com"},
}
affected, err := connector.DeleteModel(&User{}, conditions)

// Delete all records (empty conditions)
affected, err := connector.DeleteModel(&User{}, []Condition{})

// With context
affected, err := connector.DeleteModel(&User{}, conditions, WithContext(ctx))
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

Perform complex queries with joins, e.g.:

```go

// This is an example "user" representation in the database
type User struct {
	ID          uuid.UUID `gpo:"id,pk"`                          // Primary key
	Email       string    `gpo:"email,unique"`                   // Unique email
	Name        string    `gpo:"name,length(50)"`               // Max 50 characters
	Description string    `gpo:"description,nullable"`           // Nullable field
	Age         int       `gpo:"age"`                           // Regular integer field
}
// This is an example "post" representation in the database
type Post struct {
	ID       uuid.UUID `gpo:"id,pk"`                           // Primary key
	AuthorID uuid.UUID `gpo:"author_id,fk(user:id,set null)"` // FK with SET NULL on delete
	Title    string    `gpo:"title,length(200)"`               // Required title
	Content  string    `gpo:"content"`                         // TEXT field (no length limit)
	Slug     string    `gpo:"slug,unique,length(100)"`         // Unique slug with length limit
}


// This struct does not exist in database but we'll use it to hold the joined data
// we could use any name in `gpo:"<column_name>"` and then use gpo.JoinResult.ColumnMappings to map the actual database table.column to it.
// Other option is to use the same name as it has in its database table.column but then we would have to make sure 2 tables don't have overlapping column names.
	type PostWithAuthor struct {
		ID          uuid.UUID `gpo:"post_id"`
		AuthorID    string    `gpo:"author_id"`
		AuthorEmail string    `gpo:"author_email"`
		Title       string    `gpo:"title"`
		Content     string    `gpo:"content"`
		Slug        string    `gpo:"slug"`
	}

// collect results here
	var results []PostWithAuthor

 // prefix tables
	postsTable := fmt.Sprintf("%spost", connector.TablePrefix)
	usersTable := fmt.Sprintf("%suser", connector.TablePrefix)

// The actual join operation (other types of joins also available)
	err := connector.InnerJoinIntoStruct(context.Background(), &gpo.JoinResult{
		ResultModel:    &results,
		MainTableModel: &Post{},
		JoinTableModel: &User{},
		JoinCondition:  fmt.Sprintf("%s.author_id = %s.id", postsTable, usersTable),
		ColumnMappings: map[string]string{
			fmt.Sprintf("%s.id", postsTable):      "post_id",
			fmt.Sprintf("%s.id", usersTable):      "author_id",
			fmt.Sprintf("%s.email", usersTable):   "author_email",
			fmt.Sprintf("%s.title", postsTable):   "title",
			fmt.Sprintf("%s.content", postsTable): "content",
			fmt.Sprintf("%s.slug", postsTable):    "slug",
		},
	})

	if err != nil {
		panic(fmt.Sprintf("Error performing join: %v", err))
	}

	for _, post := range results {
		fmt.Printf("Post ID: %s, Author ID: %s, Author Email: %s, Title: %s, Content: %s, Slug: %s\n",
			post.ID, post.AuthorID, post.AuthorEmail, post.Title, post.Content, post.Slug)
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
    GPOTag             = "gpo"          // Unified struct tag for all field mappings
    DefaultTablePrefix = "gpo_"         // Default table prefix
    DefaultLimit       = 100            // Default query limit
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
