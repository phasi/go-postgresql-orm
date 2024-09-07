package db

type Condition struct {
	Field    string
	Operator string
	Value    interface{}
}

type DatabaseQuery struct {
	Table string
	// Fields is a slice of strings that represent the fields to be selected
	fields Fields
	// pass a struct as Model with "db_column" tags in properties
	Model           interface{}
	Condition       []Condition
	OrderBy         string
	Limit           int
	Offset          int
	Descending      bool
	AllowPagination bool
	AllowSearch     bool
	SearchText      string
	SearchFields    Fields
}

type DatabaseDelete struct {
	Table     string `json:"table"`
	Model     interface{}
	Condition []Condition
}

type DatabaseUpdate struct {
	Table     string `json:"table"`
	Fields    Fields `json:"fields"`
	Condition []Condition
}

type FieldMap map[string]string

// Column represents a column in a database table
type Column struct {
	// Name is the name of the column, for example "id"
	Name string
	// Type is the type of the column, for example "VARCHAR(255)"
	Type string
	// primaryKey is a boolean that indicates whether the column is a primary key
	PrimaryKey bool
	// allow null
	Null   bool
	Unique bool
	// Length is the length of the column, for example 255, only used for VARCHAR columns (string)
	Length int
}

type ForeignKey struct {
	ColumnName string
	References string // format: "table(column)"
	// On delete
	OnDelete string
}

// Table represents a database table
type Table struct {
	// Name is the name of the table, for example "users"
	Name string
	// Columns is a slice of Column structs that represent the columns in the table
	Columns     []Column
	ForeignKeys []ForeignKey
}

type DatabaseInsert struct {
	Fields Fields `json:"fields"`
	Table  string `json:"table"`
}

type FieldType int

const (
	IntType FieldType = iota
	StringType
	UUIDType
)

type Fields []string

func (f Fields) String() []string {
	var fields []string
	for _, field := range f {
		fields = append(fields, field)
	}
	return fields
}
