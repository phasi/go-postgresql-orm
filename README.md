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

NOTE THAT PRIMARY KEY MUST ALWAYS BE "id" in database, so where ever you put 'id' will make that column become the primary key.

| Tag name         | Description                                          |
| ---------------- | ---------------------------------------------------- |
| db_column        | Name of the column in database                       |
| db_column_length | Max length of the column                             |
| db_nullable      | Presence of this tag makes the column nullable       |
| db_unique        | Presence of this tag makes the column unique         |
| db_fk            | Foreign key to another table and field (see example) |
| db_fk_on_delete  | action when the relation is deleted e.g. "SET NULL"  |

_Example:_

```go
type TestModel struct {
	ID          uuid.UUID `db_column:"id"`
	StringValue string    `db_column:"string_value" db_column_length:"10"`
	IntValue    int       `db_column:"int_value"`
	UniqueValue string    `db_column:"unique_value" db_unique:""`
}
type TestRelatedModel struct {
	ID          uuid.UUID `db_column:"id"`
	TestModelID uuid.UUID `db_column:"test_model_id" db_fk:"orm_testmodel(id)" db_nullable:"" db_fk_on_delete:"set null"`
	StringValue string    `db_column:"string_value"`
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

### Insert a model

You can pass your model to the insert function (assuming the table was created).

_Example:_

```go
    // do insert
	err := connector.Insert(context.Background(), &TestModel{
		ID:          "4d701cf7-e218-4499-8092-7c085118e373", // can also be uuid.UUID
		StringValue: "test",
		IntValue:    10,
		UniqueValue: "thisisunique",
	})
	if err != nil {
		// handle error
	}
```

### Select a model

Selects a single row from database

_Example:_

```go
    // simulate http request (its context is automatically canceled on failed operation)
	m := &TestModel{}
	err := connector.First(context.Background(), m, "4d701cf7-e218-4499-8092-7c085118e373") // can also be uuid.UUID
	if err != nil {
		// handle error
	}
    fmt.Println(m)
```

## Want to read more?

There are more examples in the tests. There you can find how to use for example pagination, search, direction, offset and limit.
