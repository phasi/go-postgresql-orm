package db

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
)

var defaultTablePrefix = "gpo_"

type Condition struct {
	Field    string      `json:"field"`
	Operator string      `json:"operator"`
	Value    interface{} `json:"value"`
}

type DatabaseQuery struct {
	Table string `json:"table"`
	// Fields is a slice of strings that represent the fields to be selected
	Fields Fields `json:"fields"`
	// instead of Fields pass any type of struct with "db_column" tags in properties
	Model           interface{} `json:"model"`
	Condition       []Condition `json:"condition"`
	OrderBy         string      `json:"orderBy"`
	Limit           int         `json:"limit"`
	Ascending       bool        `json:"ascending"`
	AllowPagination bool        `json:"allowPagination"`
	AllowSearch     bool        `json:"allowSearch"`
	SearchFields    Fields      `json:"searchFields"`
}

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
}

type ForeignKey struct {
	ColumnName string
	References string // format: "table(column)"
}

// Table represents a database table
type Table struct {
	// Name is the name of the table, for example "users"
	Name string
	// Columns is a slice of Column structs that represent the columns in the table
	Columns     []Column
	ForeignKeys []ForeignKey
}

func convertGoTypeToPostgresType(goType string) string {
	// Convert Go type to Postgres type
	switch goType {
	case "string":
		return "VARCHAR(255)"
	case "int":
		return "INTEGER"
	case "int32":
		return "INTEGER"
	case "int64":
		return "INTEGER"
	case "uint":
		return "INTEGER"
	case "uint32":
		return "INTEGER"
	case "uint64":
		return "INTEGER"
	case "float":
		return "REAL"
	case "float32":
		return "REAL"
	case "float64":
		return "REAL"
	case "bool":
		return "BOOLEAN"
	case "uuid.UUID":
		return "UUID"
	case "time.Time":
		return "TIMESTAMP"
	default:
		return "VARCHAR(255)"
	}
}

func getColumnsAndForeignKeysFromStruct(s interface{}) ([]Column, []ForeignKey) {
	t := reflect.TypeOf(s)

	// If the type is a pointer, get the element type
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	var columns []Column
	var foreignKeys []ForeignKey

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		columnName, ok := field.Tag.Lookup("db_column")
		if ok {
			columnType := convertGoTypeToPostgresType(field.Type.Name())
			columns = append(columns, Column{Name: columnName, Type: columnType, PrimaryKey: columnName == "id"})
		}

		fk, ok := field.Tag.Lookup("db_fk")
		if ok {
			foreignKeys = append(foreignKeys, ForeignKey{ColumnName: columnName, References: fk})
		}
	}

	return columns, foreignKeys
}

func _createTable(db *sql.DB, table Table) error {
	if table.Name == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	// Start the create table statement
	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id UUID PRIMARY KEY,", table.Name)

	// Add columns to the table
	for _, column := range table.Columns {
		if column.Name == "id" {
			continue
		}
		nullText := "NOT NULL"
		if column.Null {
			nullText = "NULL"
		}
		sql += fmt.Sprintf("%s %s %s,", column.Name, column.Type, nullText)
	}

	// Add foreign keys
	for _, fk := range table.ForeignKeys {
		// Split the references into table and column
		parts := strings.SplitN(fk.References, "(", 2)
		table := parts[0]
		column := strings.TrimSuffix(parts[1], ")")

		// Correctly format the REFERENCES clause
		sql += fmt.Sprintf("FOREIGN KEY (%s) REFERENCES %s(%s),", fk.ColumnName, table, column)
	}

	// Remove trailing comma and close parentheses
	sql = strings.TrimSuffix(sql, ",") + ")"

	// Execute the create table statement
	_, err := db.Exec(sql)
	if err != nil {
		return err
	}

	return nil
}

type DatabaseInsert struct {
	Fields Fields `json:"fields"`
	Table  string `json:"table"`
}

type DatabaseHandler interface {
	Query(r *http.Request, queryProps DatabaseQuery) (data []map[string]interface{}, err error)
	Insert(ctx context.Context, insertProps DatabaseInsert, data *[]map[string]interface{}) (err error)
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

type SQLConnector struct {
	DriverName     string
	DatasourceName string
	db             *sql.DB
	TablePrefix    string
}

func (s *SQLConnector) Connect() (err error) {
	s.db, err = sql.Open(s.DriverName, s.DatasourceName)
	return err
}

func (s *SQLConnector) Close() error {
	return s.db.Close()
}

func (s *SQLConnector) GetConnection() *sql.DB {
	return s.db
}

func (s *SQLConnector) Ping() error {
	db := s.GetConnection()
	return db.Ping()
}

func (s *SQLConnector) CreateDatabase(dbName string) error {
	db := s.GetConnection()
	// Check if the database exists
	var exists bool
	db.QueryRow("SELECT 1 FROM pg_database WHERE datname=$1", dbName).Scan(&exists)
	if exists {
		return nil
	}

	// If not, create it
	_, err := db.Exec(fmt.Sprintf("CREATE DATABASE %s", dbName))
	return err
}

// CreateTables creates tables in the database for the given models (table names are populated from the struct names)
func (s *SQLConnector) CreateTables(models []interface{}) error {
	for _, model := range models {
		err := s.CreateTable(model)
		if err != nil {
			return err
		}
	}
	return nil
}

func getTableNameFromModel(tablePrefix string, model interface{}) string {
	modelType := reflect.TypeOf(model)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	modelName := modelType.Name()
	tableName := strings.ToLower(modelName)
	tPrefix := tablePrefix
	if tPrefix == "" {
		tPrefix = defaultTablePrefix
	}
	return fmt.Sprintf("%s%s", tPrefix, tableName)
}

// CreateTable creates a single table in the database for the given model (table name is passed as an argument)
func (s *SQLConnector) CreateTable(model interface{}) error {
	tableName := getTableNameFromModel(s.TablePrefix, model)
	columns, foreignKeys := getColumnsAndForeignKeysFromStruct(model)
	table := Table{Name: tableName, Columns: columns, ForeignKeys: foreignKeys}
	db := s.GetConnection()
	return _createTable(db, table)
}

func (s *SQLConnector) buildQuery(params *DatabaseQuery) string {
	parseTags(params.Model, &params.Fields)
	var query string
	query = fmt.Sprintf("SELECT %s FROM %s", strings.Join(params.Fields.String(), ","), params.Table)
	if len(params.Condition) > 0 {
		query += " WHERE "
		for i, condition := range params.Condition {
			if i > 0 {
				query += " AND "
			}
			if condition.Operator == "LIKE" || condition.Operator == "NOT LIKE" {
				query += fmt.Sprintf("%s %s '%%%s%%'", condition.Field, condition.Operator, condition.Value)
			} else {
				switch v := condition.Value.(type) {
				case int, int32, int64, float32, float64:
					query += fmt.Sprintf("%s %s %v", condition.Field, condition.Operator, v)
				default:
					query += fmt.Sprintf("%s %s '%s'", condition.Field, condition.Operator, condition.Value)
				}
			}
		}
	}
	if params.OrderBy != "" {
		query += fmt.Sprintf(" ORDER BY %s", params.OrderBy)
		if params.Ascending {
			query += " ASC"
		} else {
			query += " DESC"
		}
	}
	if params.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", params.Limit)
	}
	return query
}

func (s *SQLConnector) buildAdvancedQuery(params *DatabaseQuery, limit int, offset int, orderBy string, searchText string) string {
	parseTags(params.Model, &params.Fields)
	var query string
	query = fmt.Sprintf("SELECT %s FROM %s", strings.Join(params.Fields.String(), ","), params.Table)
	if len(params.Condition) > 0 || len(params.SearchFields) > 0 {
		query += " WHERE "
		for i, condition := range params.Condition {
			if i > 0 {
				query += " AND "
			}
			if condition.Operator == "LIKE" || condition.Operator == "NOT LIKE" {
				query += fmt.Sprintf("%s %s '%%%s%%'", condition.Field, condition.Operator, condition.Value)
			} else {
				switch v := condition.Value.(type) {
				case int, int32, int64, float32, float64:
					query += fmt.Sprintf("%s %s %v", condition.Field, condition.Operator, v)
				default:
					query += fmt.Sprintf("%s %s '%s'", condition.Field, condition.Operator, condition.Value)
				}
			}
		}
		for i, field := range params.SearchFields {
			if len(params.Condition) > 0 || i > 0 {
				query += " OR "
			}
			query += fmt.Sprintf("%s LIKE '%%%s%%'", field, searchText)
		}
	}
	ob := params.OrderBy
	if orderBy != "" {
		ob = orderBy
	}
	if ob != "" {
		query += fmt.Sprintf(" ORDER BY %s", ob)
		if params.Ascending {
			query += " ASC"
		} else {
			query += " DESC"
		}
	}
	if params.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", params.Limit)
	} else if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	} else {
		query += " LIMIT 10"
	}
	if offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", offset)
	}
	return query
}

func (s *SQLConnector) buildInsertQuery(params *DatabaseInsert, model interface{}) (string, []interface{}, error) {
	var query string
	query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (", params.Table, strings.Join(params.Fields.String(), ","))
	vals := make([]interface{}, len(params.Fields))
	modelValue := reflect.ValueOf(model)
	if modelValue.Kind() == reflect.Ptr {
		modelValue = modelValue.Elem()
	}
	t := modelValue.Type()
	for i := 0; i < len(params.Fields); i++ {
		dbColumnName := params.Fields[i]
		var structFieldName string
		for j := 0; j < t.NumField(); j++ {
			field := t.Field(j)
			if field.Tag.Get("db_column") == dbColumnName {
				structFieldName = field.Name
				break
			}
		}
		if structFieldName == "" {
			return "", nil, fmt.Errorf("no struct field found for database column %s", dbColumnName)
		}
		field := modelValue.FieldByName(structFieldName)
		vals[i] = field.Interface()
		query += fmt.Sprintf("$%d", i+1)
		if i < len(params.Fields)-1 {
			query += ","
		}
	}
	query += ")"
	return query, vals, nil
}

func (s SQLConnector) Insert(r *http.Request, model interface{}) (err error) {
	insertStmt := DatabaseInsert{
		Table: getTableNameFromModel(s.TablePrefix, model),
	}
	parseTags(model, &insertStmt.Fields)
	q, args, err := s.buildInsertQuery(&insertStmt, model)
	if err != nil {
		return
	}
	db, err := sql.Open(s.DriverName, s.DatasourceName)
	if err != nil {
		return
	}
	defer db.Close()

	// Prepare the query
	stmt, err := db.PrepareContext(r.Context(), q)
	if err != nil {
		return
	}
	defer stmt.Close()

	// Execute the query
	_, err = stmt.Exec(args...)
	return
}

type FieldMap map[string]string

func parseTags(model interface{}, fields *Fields) FieldMap {
	val := reflect.ValueOf(model)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	fieldMap := make(FieldMap)
	for i := 0; i < val.NumField(); i++ {
		typeField := val.Type().Field(i)
		if tag, ok := typeField.Tag.Lookup("db_column"); ok {
			*fields = append(*fields, tag)
			fieldMap[tag] = typeField.Name
		}
	}
	return fieldMap
}

func (s SQLConnector) First(r *http.Request, tableName string, model interface{}, conditionOrId interface{}) error {
	if conditionOrId == nil {
		return fmt.Errorf("conditionOrId cannot be nil")
	}
	var condition []Condition
	switch v := conditionOrId.(type) {
	case []Condition:
		condition = v
	default:
		condition = []Condition{
			{
				Field:    "id",
				Operator: "=",
				Value:    v,
			},
		}
	}
	var queryProps DatabaseQuery
	queryProps.Table = tableName
	if tableName == "" {
		queryProps.Table = getTableNameFromModel(s.TablePrefix, model)
	}
	queryProps.Model = model
	queryProps.Condition = condition
	queryProps.Limit = 1
	fieldMap := parseTags(model, &queryProps.Fields)
	rows, err := s.doQuery(r, &queryProps)
	if err != nil {
		return fmt.Errorf("error querying database: %v", err)
	}
	defer rows.Close()
	if rows.Next() {
		columns, _ := rows.Columns()
		scanArgs := make([]interface{}, len(columns))
		val := reflect.ValueOf(model).Elem()
		for i, column := range columns {
			if field, ok := fieldMap[column]; ok {
				fieldVal := val.FieldByName(field)
				if fieldVal.IsValid() && fieldVal.CanAddr() {
					scanArgs[i] = fieldVal.Addr().Interface()
				} else {
					var discard interface{}
					scanArgs[i] = &discard
				}
			} else {
				var discard interface{}
				scanArgs[i] = &discard
			}
		}
		err = rows.Scan(scanArgs...)
		if err != nil {
			return fmt.Errorf("error scanning row: %v", err)
		}
	}
	return nil
}

func (s SQLConnector) All(r *http.Request, queryProps *DatabaseQuery) ([]interface{}, error) {
	if queryProps.Table == "" && queryProps.Model != nil {
		queryProps.Table = getTableNameFromModel(s.TablePrefix, queryProps.Model)
	}
	fieldMap := parseTags(queryProps.Model, &queryProps.Fields)
	rows, err := s.doQuery(r, queryProps)
	if err != nil {
		return nil, fmt.Errorf("error querying database: %v", err)
	}
	defer rows.Close()

	var results []interface{}
	columns, _ := rows.Columns()
	for rows.Next() {
		scanArgs := make([]interface{}, len(columns))
		val := reflect.New(reflect.TypeOf(queryProps.Model).Elem())
		for i, column := range columns {
			if field, ok := fieldMap[column]; ok {
				fieldVal := val.Elem().FieldByName(field)
				if fieldVal.IsValid() && fieldVal.CanAddr() {
					scanArgs[i] = fieldVal.Addr().Interface()
				} else {
					var discard interface{}
					scanArgs[i] = &discard
				}
			} else {
				var discard interface{}
				scanArgs[i] = &discard
			}
		}
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, fmt.Errorf("error scanning row: %v", err)
		}
		results = append(results, val.Interface())
	}
	return results, nil
}

func (s SQLConnector) Query(r *http.Request, queryProps *DatabaseQuery) ([]interface{}, error) {
	var fieldMap FieldMap
	if queryProps.Model != nil {
		fieldMap = parseTags(queryProps.Model, &queryProps.Fields)
	}
	rows, err := s.doQuery(r, queryProps)
	if err != nil {
		return nil, fmt.Errorf("error querying database: %v", err)
	}
	defer rows.Close()

	var results []interface{}
	columns, _ := rows.Columns()
	for rows.Next() {
		scanArgs := make([]interface{}, len(columns))
		val := reflect.New(reflect.TypeOf(queryProps.Model).Elem())
		for i, column := range columns {
			if field, ok := fieldMap[column]; ok {
				fieldVal := val.Elem().FieldByName(field)
				if fieldVal.IsValid() && fieldVal.CanAddr() {
					scanArgs[i] = fieldVal.Addr().Interface()
				} else {
					var discard interface{}
					scanArgs[i] = &discard
				}
			} else {
				var discard interface{}
				scanArgs[i] = &discard
			}
		}
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, fmt.Errorf("error scanning row: %v", err)
		}
		results = append(results, val.Interface())
	}
	return results, nil
}

func (s SQLConnector) doQuery(r *http.Request, queryProps *DatabaseQuery) (rows *sql.Rows, err error) {
	var q string
	if queryProps.AllowPagination || queryProps.AllowSearch {
		// get "orderBy" from request query params
		orderBy := r.URL.Query().Get("orderBy")
		// iterate over endpoint.Query.Fields to check if orderBy is valid
		if orderBy != "" {
			found := false
			for _, field := range queryProps.Fields {
				if field == orderBy {
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("invalid field '%s' used for orderBy", orderBy)
			}
		}

		// get "offset" from request query params
		offset := 0
		if r.URL.Query().Get("offset") != "" {
			offset, err = strconv.Atoi(r.URL.Query().Get("offset"))
			if err != nil {
				return nil, err
			}
		}
		// get "limit" from request query params
		limit := 0
		if r.URL.Query().Get("limit") != "" {
			limit, err = strconv.Atoi(r.URL.Query().Get("limit"))
			if err != nil {
				return nil, err
			}
		}

		searchText := r.URL.Query().Get("search")

		q = s.buildAdvancedQuery(queryProps, limit, offset, orderBy, searchText)
	} else {
		q = s.buildQuery(queryProps)
	}
	db := s.GetConnection()
	// Perform a query
	rows, err = db.QueryContext(r.Context(), q)
	if err != nil {
		return nil, err
	}
	return rows, nil
}
