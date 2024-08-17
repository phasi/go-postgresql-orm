package db

import (
	"database/sql"
	"fmt"
	"net/http"
	"reflect"
	"strconv"

	_ "github.com/lib/pq"
)

const defaultTablePrefix = "gpo_"

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

// CreateTable creates a single table in the database for the given model
func (s *SQLConnector) CreateTable(model interface{}) error {
	tableName := getTableNameFromModel(s.TablePrefix, model)
	columns, foreignKeys := getColumnsAndForeignKeysFromStruct(model)
	table := Table{Name: tableName, Columns: columns, ForeignKeys: foreignKeys}
	db := s.GetConnection()
	return _createTable(db, table)
}

func (s *SQLConnector) DropTable(modelOrTableName interface{}, cascade bool) error {
	var tableName string
	switch v := modelOrTableName.(type) {
	case string:
		tableName = v
	default:
		tableName = getTableNameFromModel(s.TablePrefix, v)
	}

	sql := fmt.Sprintf("DROP TABLE %s", tableName)

	if cascade {
		sql += " CASCADE"
	}

	db := s.GetConnection()
	_, err := db.Exec(sql)
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

func (s *SQLConnector) DropTables(modelsOrTableNames ...interface{}) error {
	for _, modelOrTableName := range modelsOrTableNames {
		err := s.DropTable(modelOrTableName, true) // true for CASCADE
		if err != nil {
			return err
		}
	}
	return nil
}

func (s SQLConnector) Insert(r *http.Request, model interface{}) (err error) {
	insertStmt := DatabaseInsert{
		Table: getTableNameFromModel(s.TablePrefix, model),
	}
	parseTags(model, &insertStmt.Fields)
	q, args, err := buildInsertStmt(&insertStmt, model)
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
	var args []interface{}
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
			args = append(args, condition.Value)
		}
	}
	// Prepare the statement
	stmt, err := db.Prepare(sql)
	if err != nil {
		return err
	}
	defer stmt.Close()

	// Execute the delete statement
	result, err := stmt.Exec(args...)
	if err != nil {
		return err
	}
	affectedRows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affectedRows == 0 {
		fmt.Println("no rows affected, did you provide the correct condition?")
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

func (s SQLConnector) Update(r *http.Request, model interface{}) (int64, error) {
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
	q, args, err := buildUpdateStmt(&updateStmt, model)
	if err != nil {
		return 0, err
	}
	db, err := sql.Open(s.DriverName, s.DatasourceName)
	if err != nil {
		return 0, err
	}
	defer db.Close()

	// Prepare the query
	stmt, err := db.PrepareContext(r.Context(), q)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	// Execute the query
	result, err := stmt.Exec(args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
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

		q, args = buildAdvancedQuery(queryProps, limit, offset, orderBy, searchText)
	} else {
		q, args = buildQuery(queryProps)
	}
	db := s.GetConnection()
	// Perform a query
	rows, err = db.QueryContext(r.Context(), q, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}
