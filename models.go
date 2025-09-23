package db

import (
	"context"
	"database/sql"
)

const (
	DefaultIDField     = "id"
	GPOTag             = "gpo"
	DefaultLimit       = 100
	DefaultTablePrefix = "gpo_"
)

// GPOField represents parsed field information from a gpo tag
type GPOField struct {
	ColumnName   string
	IsPrimaryKey bool
	IsUnique     bool
	IsNullable   bool
	Length       int
	ForeignKey   *ForeignKeyInfo
}

// ForeignKeyInfo represents foreign key relationship information
type ForeignKeyInfo struct {
	Table    string
	Column   string
	OnDelete string
}

// Option represents a configuration option for database operations
type Option func(*Config)

// Config holds configuration for database operations
type Config struct {
	ctx context.Context
	tx  *sql.Tx
}

// WithContext sets the context for database operations
func WithContext(ctx context.Context) Option {
	return func(c *Config) { c.ctx = ctx }
}

// WithTransaction sets the transaction for database operations
func WithTransaction(tx *sql.Tx) Option {
	return func(c *Config) { c.tx = tx }
}

type Condition struct {
	Field    string
	Operator string
	Value    interface{}
}

type DatabaseQuery struct {
	Table string
	// Fields is a slice of strings that represent the fields to be selected
	fields          Fields
	Conditions      []Condition
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
	Table      string `json:"table"`
	Conditions []Condition
}

type DatabaseUpdate struct {
	Table      string `json:"table"`
	Fields     Fields `json:"fields"`
	Conditions []Condition
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
	Null bool
	// unique value
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

type JoinType string

const (
	InnerJoin JoinType = "INNER JOIN"
	LeftJoin  JoinType = "LEFT JOIN"
	RightJoin JoinType = "RIGHT JOIN"
	FullJoin  JoinType = "FULL OUTER JOIN"
)

type JoinProps struct {
	MainTableModel  interface{}
	JoinTableModel  interface{}
	MainTableCols   []string
	JoinTableCols   []string
	JoinCondition   string
	WhereConditions []Condition
	JoinType        JoinType // Required field - no default
}

// JoinResult represents the result of a join operation that can be scanned into structs
type JoinResult struct {
	ResultModel     interface{} // The struct to scan results into (should be a slice pointer)
	MainTableModel  interface{}
	JoinTableModel  interface{}
	JoinCondition   string
	WhereConditions []Condition
	JoinType        JoinType // Required field - no default
	// ColumnMappings maps database columns to struct field names for complex joins
	ColumnMappings map[string]string // Optional: "table.column" -> "struct_field_db_tag"
}
