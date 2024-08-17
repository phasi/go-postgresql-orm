package db

import (
	"database/sql"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
)

var defaultTablePrefix = "gpo_"

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
	Descending      bool
	AllowPagination bool
	AllowSearch     bool
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

func convertGoTypeToPostgresType(goType string, length int) string {
	// Convert Go type to Postgres type
	switch goType {
	case "string":
		if length > 0 {
			return fmt.Sprintf("VARCHAR(%d)", length)
		}
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
	case "UUID":
		return "UUID"
	case "Time":
		return "TIMESTAMP"
	default:
		if length > 0 {
			return fmt.Sprintf("VARCHAR(%d)", length)
		}
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
			columnLength, isSet := field.Tag.Lookup("db_column_length")
			length := 0
			if isSet {
				var err error
				length, err = strconv.Atoi(columnLength)
				if err != nil {
					fmt.Println("error converting column length to int, using max length of 255")
				}
			}
			columnType := convertGoTypeToPostgresType(field.Type.Name(), length)
			_, unique := field.Tag.Lookup("db_unique")
			columns = append(columns, Column{Name: columnName, Type: columnType, PrimaryKey: columnName == "id", Unique: unique})
		}

		fk, ok := field.Tag.Lookup("db_fk")
		if ok {
			onDeleteVal, onDeleteFound := field.Tag.Lookup("db_fk_on_delete")
			if onDeleteFound {
				foreignKeys = append(foreignKeys, ForeignKey{ColumnName: columnName, References: fk, OnDelete: onDeleteVal})
			} else {
				foreignKeys = append(foreignKeys, ForeignKey{ColumnName: columnName, References: fk})
			}
		}
	}

	return columns, foreignKeys
}

func validateOnDeleteText(text string) bool {
	switch strings.ToUpper(text) {
	case "NO ACTION", "RESTRICT", "CASCADE", "SET NULL", "SET DEFAULT":
		return true
	}
	return false
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
		uniqueText := ""
		if column.Unique {
			uniqueText = "UNIQUE"
		}
		sql += fmt.Sprintf("%s %s %s %s,", column.Name, column.Type, nullText, uniqueText)
	}

	// Add foreign keys
	for _, fk := range table.ForeignKeys {
		// Split the references into table and column
		parts := strings.SplitN(fk.References, "(", 2)
		table := parts[0]
		column := strings.TrimSuffix(parts[1], ")")

		// Check if the ON DELETE clause is set
		onDeleteText := ""
		if fk.OnDelete != "" {
			if !validateOnDeleteText(fk.OnDelete) {
				return fmt.Errorf("invalid ON DELETE clause: %s", fk.OnDelete)
			}
			onDeleteText = fmt.Sprintf(" ON DELETE %s", strings.ToUpper(fk.OnDelete))
		}

		// Correctly format the REFERENCES clause
		sql += fmt.Sprintf("FOREIGN KEY (%s) REFERENCES %s(%s)%s,", fk.ColumnName, table, column, onDeleteText)
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
func (s *SQLConnector) CreateTables(models ...interface{}) error {
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

// CreateTable creates a single table in the database for the given model
func (s *SQLConnector) CreateTable(model interface{}) error {
	tableName := getTableNameFromModel(s.TablePrefix, model)
	columns, foreignKeys := getColumnsAndForeignKeysFromStruct(model)
	table := Table{Name: tableName, Columns: columns, ForeignKeys: foreignKeys}
	db := s.GetConnection()
	return _createTable(db, table)
}

func (s *SQLConnector) buildQuery(params *DatabaseQuery) (string, []interface{}) {
	parseTags(params.Model, &params.fields)
	var query string
	args := make([]interface{}, 0)
	query = fmt.Sprintf("SELECT %s FROM %s", strings.Join(params.fields.String(), ","), params.Table)
	if len(params.Condition) > 0 {
		query += " WHERE "
		for i, condition := range params.Condition {
			if i > 0 {
				query += " AND "
			}
			if condition.Operator == "LIKE" || condition.Operator == "NOT LIKE" {
				query += fmt.Sprintf("%s %s $%d", condition.Field, condition.Operator, len(args)+1)
				args = append(args, "%"+condition.Value.(string)+"%")
			} else {
				switch v := condition.Value.(type) {
				case int, int32, int64, float32, float64:
					query += fmt.Sprintf("%s %s $%d", condition.Field, condition.Operator, len(args)+1)
					args = append(args, v)
				default:
					query += fmt.Sprintf("%s %s $%d", condition.Field, condition.Operator, len(args)+1)
					args = append(args, condition.Value)
				}
			}
		}
	}
	if params.OrderBy != "" {
		query += fmt.Sprintf(" ORDER BY %s", params.OrderBy)
		if params.Descending {
			query += " DESC"
		} else {
			query += " ASC"
		}
	}
	if params.Limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", len(args)+1)
		args = append(args, params.Limit)
	}
	return query, args
}

func (s *SQLConnector) buildAdvancedQuery(params *DatabaseQuery, limit int, offset int, orderBy string, searchText string) (string, []interface{}) {
	parseTags(params.Model, &params.fields)
	var query string
	args := make([]interface{}, 0)
	query = fmt.Sprintf("SELECT %s FROM %s", strings.Join(params.fields.String(), ","), params.Table)
	if len(params.Condition) > 0 || len(params.SearchFields) > 0 {
		query += " WHERE "
		for i, condition := range params.Condition {
			if i > 0 {
				query += " AND "
			}
			if condition.Operator == "LIKE" || condition.Operator == "NOT LIKE" {
				query += fmt.Sprintf("%s %s $%d", condition.Field, condition.Operator, len(args)+1)
				args = append(args, "%"+condition.Value.(string)+"%")
			} else {
				switch v := condition.Value.(type) {
				case int, int32, int64, float32, float64:
					query += fmt.Sprintf("%s %s $%d", condition.Field, condition.Operator, len(args)+1)
					args = append(args, v)
				default:
					query += fmt.Sprintf("%s %s $%d", condition.Field, condition.Operator, len(args)+1)
					args = append(args, condition.Value)
				}
			}
		}
		for i, field := range params.SearchFields {
			if len(params.Condition) > 0 || i > 0 {
				query += " OR "
			}
			query += fmt.Sprintf("%s LIKE $%d", field, len(args)+1)
			args = append(args, "%"+searchText+"%")
		}
	}
	ob := params.OrderBy
	if orderBy != "" {
		ob = orderBy
	}
	if ob != "" {
		query += fmt.Sprintf(" ORDER BY %s", ob)
		if params.Descending {
			query += " DESC"
		} else {
			query += " ASC"
		}
	}
	if params.Limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", len(args)+1)
		args = append(args, params.Limit)
	} else if limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", len(args)+1)
		args = append(args, limit)
	} else {
		query += " LIMIT 10"
	}
	if offset > 0 {
		query += fmt.Sprintf(" OFFSET $%d", len(args)+1)
		args = append(args, offset)
	}
	return query, args
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

func (s SQLConnector) First(r *http.Request, model interface{}, conditionOrId interface{}) error {
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
	queryProps.Table = getTableNameFromModel(s.TablePrefix, model)
	queryProps.Model = model
	queryProps.Condition = condition
	queryProps.Limit = 1
	fieldMap := parseTags(model, &queryProps.fields)
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
func (s SQLConnector) All(r *http.Request, models interface{}, queryProps *DatabaseQuery) error {
	// Ensure models is a pointer to a slice
	val := reflect.ValueOf(models)
	if val.Kind() != reflect.Ptr || val.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("error handling %s: models must be a pointer to a slice", val.Type())
	}

	if queryProps.Table == "" && queryProps.Model != nil {
		queryProps.Table = getTableNameFromModel(s.TablePrefix, queryProps.Model)
	}
	fieldMap := parseTags(queryProps.Model, &queryProps.fields)
	rows, err := s.doQuery(r, queryProps)
	if err != nil {
		return fmt.Errorf("error querying database: %v", err)
	}
	defer rows.Close()
	columns, _ := rows.Columns()

	// scan rows into "models" slice
	for rows.Next() {
		scanArgs := make([]interface{}, len(columns))
		modelType := reflect.TypeOf(queryProps.Model).Elem()
		modelVal := reflect.New(modelType)
		for i, column := range columns {
			if field, ok := fieldMap[column]; ok {
				fieldVal := modelVal.Elem().FieldByName(field)
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
		val.Elem().Set(reflect.Append(val.Elem(), modelVal.Elem()))
	}
	return nil
}

func (s SQLConnector) Query(r *http.Request, queryProps *DatabaseQuery) ([]interface{}, error) {
	var fieldMap FieldMap
	if queryProps.Model != nil {
		fieldMap = parseTags(queryProps.Model, &queryProps.fields)
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

func (s SQLConnector) Delete(r *http.Request, model interface{}, condition ...Condition) error {
	deleteStmt := DatabaseDelete{
		Table:     getTableNameFromModel(s.TablePrefix, model),
		Condition: condition,
	}

	db, err := sql.Open(s.DriverName, s.DatasourceName)
	if err != nil {
		return err
	}
	defer db.Close()

	// Start the delete statement
	sql := fmt.Sprintf("DELETE FROM %s", deleteStmt.Table)

	// Add the conditions
	if len(deleteStmt.Condition) > 0 {
		sql += " WHERE "
		for i, condition := range deleteStmt.Condition {
			if i > 0 {
				sql += " AND "
			}
			if condition.Operator == "LIKE" || condition.Operator == "NOT LIKE" {
				sql += fmt.Sprintf("%s %s $%d", condition.Field, condition.Operator, i+1)
			} else {
				switch condition.Value.(type) {
				case int, int32, int64, float32, float64:
					sql += fmt.Sprintf("%s %s $%d", condition.Field, condition.Operator, i+1)
				default:
					sql += fmt.Sprintf("%s %s $%d", condition.Field, condition.Operator, i+1)
				}
			}
		}
	}
	// Execute the delete statement
	_, err = db.Exec(sql)
	if err != nil {
		return err
	}

	return nil
}

func (s SQLConnector) DeleteById(r *http.Request, model interface{}, id interface{}) error {
	return s.Delete(r, model, Condition{
		Field:    "id",
		Operator: "=",
		Value:    id,
	})
}

func (s SQLConnector) Update(r *http.Request, model interface{}) error {
	updateStmt := DatabaseUpdate{
		Table: getTableNameFromModel(s.TablePrefix, model),
	}
	parseTags(model, &updateStmt.Fields)
	val := reflect.ValueOf(model)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	t := val.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.Tag.Get("db_column") == "id" {
			updateStmt.Condition = []Condition{
				{
					Field:    "id",
					Operator: "=",
					Value:    val.Field(i).Interface(),
				},
			}
			break
		}
	}
	q, args, err := s.buildUpdateString(&updateStmt, model)
	if err != nil {
		return err
	}
	db, err := sql.Open(s.DriverName, s.DatasourceName)
	if err != nil {
		return err
	}
	defer db.Close()

	// Prepare the query
	stmt, err := db.PrepareContext(r.Context(), q)
	if err != nil {
		return err
	}
	defer stmt.Close()

	// Execute the query
	_, err = stmt.Exec(args...)
	return err
}

func (s *SQLConnector) buildUpdateString(params *DatabaseUpdate, model interface{}) (string, []interface{}, error) {
	var query string
	query = fmt.Sprintf("UPDATE %s SET ", params.Table)
	val := reflect.ValueOf(model)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	t := val.Type()
	args := make([]interface{}, 0)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.Tag.Get("db_column") == "id" {
			continue
		}
		if field.Tag.Get("db_column") == "" {
			return "", nil, fmt.Errorf("no db_column tag found for field %s", field.Name)
		}
		query += fmt.Sprintf("%s = $%d, ", field.Tag.Get("db_column"), len(args)+1)
		args = append(args, val.Field(i).Interface())
	}
	query = strings.TrimSuffix(query, ", ")
	if len(params.Condition) > 0 {
		query += " WHERE "
		for i, condition := range params.Condition {
			if i > 0 {
				query += " AND "
			}
			if condition.Operator == "LIKE" || condition.Operator == "NOT LIKE" {
				query += fmt.Sprintf("%s %s $%d", condition.Field, condition.Operator, len(args)+1)
				args = append(args, "%"+condition.Value.(string)+"%")
			} else {
				switch v := condition.Value.(type) {
				case int, int32, int64, float32, float64:
					query += fmt.Sprintf("%s %s $%d", condition.Field, condition.Operator, len(args)+1)
					args = append(args, v)
				default:
					query += fmt.Sprintf("%s %s $%d", condition.Field, condition.Operator, len(args)+1)
					args = append(args, condition.Value)
				}
			}
		}
	}
	return query, args, nil
}

func (s SQLConnector) doQuery(r *http.Request, queryProps *DatabaseQuery) (rows *sql.Rows, err error) {
	var q string
	var args []interface{}
	if queryProps.AllowPagination || queryProps.AllowSearch {
		// get "orderBy" from request query params
		orderBy := r.URL.Query().Get("orderBy")
		// iterate over endpoint.Query.Fields to check if orderBy is valid
		if orderBy != "" {
			found := false
			for _, field := range queryProps.fields {
				if field == orderBy {
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("invalid field '%s' used for orderBy", orderBy)
			}
		}

		direction := r.URL.Query().Get("direction")
		if direction != "" {
			if direction != "asc" && direction != "desc" {
				return nil, fmt.Errorf("invalid direction '%s' used for orderBy", direction)
			} else if direction == "desc" {
				queryProps.Descending = true
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

		q, args = s.buildAdvancedQuery(queryProps, limit, offset, orderBy, searchText)
	} else {
		q, args = s.buildQuery(queryProps)
	}
	db := s.GetConnection()
	// Perform a query
	rows, err = db.QueryContext(r.Context(), q, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}
