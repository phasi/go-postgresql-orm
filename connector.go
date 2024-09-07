package db

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	_ "github.com/lib/pq"
)

const defaultTablePrefix = "gpo_"

type PostgreSQLConnector struct {
	Host        string  `json:"host"`
	Port        string  `json:"port"`
	User        string  `json:"user"`
	Password    string  `json:"password"`
	Database    string  `json:"database"`
	SSLMode     string  `json:"sslmode"` // options: verify-full, verify-ca, disable
	db          *sql.DB // db connection
	TablePrefix string
}

func (s *PostgreSQLConnector) getConnectionString() string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		s.Host,
		s.Port,
		s.User,
		s.Password,
		s.Database,
		s.SSLMode,
	)
}

func (s *PostgreSQLConnector) CloseConnection() {
	if s.db != nil {
		s.db.Close()
	}
}

func (s *PostgreSQLConnector) Connect() (err error) {
	s.db, err = sql.Open("postgres", s.getConnectionString())
	return err
}

func (s *PostgreSQLConnector) Close() error {
	return s.db.Close()
}

func (s *PostgreSQLConnector) GetConnection() *sql.DB {
	return s.db
}

func (s *PostgreSQLConnector) Ping() error {
	db := s.GetConnection()
	return db.Ping()
}

func (s *PostgreSQLConnector) CreateDatabase(dbName string) error {
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
func (s *PostgreSQLConnector) CreateTable(model interface{}) error {
	tableName := getTableNameFromModel(s.TablePrefix, model)
	columns, foreignKeys := getColumnsAndForeignKeysFromStruct(model)
	table := Table{Name: tableName, Columns: columns, ForeignKeys: foreignKeys}
	db := s.GetConnection()
	return _createTable(db, table)
}

func (s *PostgreSQLConnector) DropTable(modelOrTableName interface{}, cascade bool) error {
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
func (s *PostgreSQLConnector) CreateTables(models ...interface{}) error {
	for _, model := range models {
		err := s.CreateTable(model)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *PostgreSQLConnector) DropTables(modelsOrTableNames ...interface{}) error {
	for _, modelOrTableName := range modelsOrTableNames {
		err := s.DropTable(modelOrTableName, true) // true for CASCADE
		if err != nil {
			return err
		}
	}
	return nil
}

func (s PostgreSQLConnector) Insert(ctx context.Context, model interface{}) (err error) {
	insertStmt := DatabaseInsert{
		Table: getTableNameFromModel(s.TablePrefix, model),
	}
	parseTags(model, &insertStmt.Fields)
	q, args, err := buildInsertStmt(&insertStmt, model)
	if err != nil {
		return
	}
	db := s.GetConnection()
	// Prepare the query
	stmt, err := db.PrepareContext(ctx, q)
	if err != nil {
		return
	}
	defer stmt.Close()
	// Execute the query
	_, err = stmt.ExecContext(ctx, args...)
	return
}

func (s PostgreSQLConnector) First(ctx context.Context, model interface{}, conditionOrId interface{}) error {
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
	rows, err := s.doQuery(ctx, &queryProps)
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
func (s PostgreSQLConnector) All(ctx context.Context, models interface{}, queryProps *DatabaseQuery) error {
	// Ensure models is a pointer to a slice
	val := reflect.ValueOf(models)
	if val.Kind() != reflect.Ptr || val.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("error handling %s: models must be a pointer to a slice", val.Type())
	}

	if queryProps.Table == "" && queryProps.Model != nil {
		queryProps.Table = getTableNameFromModel(s.TablePrefix, queryProps.Model)
	}
	fieldMap := parseTags(queryProps.Model, &queryProps.fields)
	rows, err := s.doQuery(ctx, queryProps)
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

func (s PostgreSQLConnector) Query(ctx context.Context, queryProps *DatabaseQuery) ([]interface{}, error) {
	var fieldMap FieldMap
	if queryProps.Model != nil {
		fieldMap = parseTags(queryProps.Model, &queryProps.fields)
	}
	rows, err := s.doQuery(ctx, queryProps)
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

func (s PostgreSQLConnector) Delete(ctx context.Context, model interface{}, condition ...Condition) error {
	deleteStmt := DatabaseDelete{
		Table:     getTableNameFromModel(s.TablePrefix, model),
		Condition: condition,
	}

	db := s.GetConnection()

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

func (s PostgreSQLConnector) DeleteById(ctx context.Context, model interface{}, id interface{}) error {
	return s.Delete(ctx, model, Condition{
		Field:    "id",
		Operator: "=",
		Value:    id,
	})
}

func (s PostgreSQLConnector) Update(ctx context.Context, model interface{}) (int64, error) {
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
	db := s.GetConnection()

	// Prepare the query
	stmt, err := db.PrepareContext(ctx, q)
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

func (s PostgreSQLConnector) doQuery(ctx context.Context, queryProps *DatabaseQuery) (rows *sql.Rows, err error) {
	var q string
	var args []interface{}
	if queryProps.AllowPagination || queryProps.AllowSearch {
		q, args = buildAdvancedQuery(queryProps)
	} else {
		q, args = buildQuery(queryProps)
	}
	db := s.GetConnection()
	// Perform a query
	rows, err = db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}
