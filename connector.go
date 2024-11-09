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

func (s PostgreSQLConnector) InsertWithContext(ctx context.Context, model interface{}) (err error) {
	return s.insert(ctx, model)
}

func (s PostgreSQLConnector) Insert(model interface{}) (err error) {
	return s.insert(context.Background(), model)
}

func (s PostgreSQLConnector) insert(ctx context.Context, model interface{}) (err error) {
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

func (s PostgreSQLConnector) FirstWithContext(ctx context.Context, model interface{}, conditionOrId interface{}) error {
	return s.first(ctx, model, conditionOrId)
}

func (s PostgreSQLConnector) First(model interface{}, conditionOrId interface{}) error {
	return s.first(context.Background(), model, conditionOrId)
}

func (s PostgreSQLConnector) first(ctx context.Context, model interface{}, conditionOrId interface{}) error {
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

func (s PostgreSQLConnector) FirstWithTransactionAndContext(ctx context.Context, tx *sql.Tx, model interface{}, conditionOrId interface{}) error {
	return s.firstWithTransaction(ctx, tx, model, conditionOrId)
}

func (s PostgreSQLConnector) FirstWithTransaction(tx *sql.Tx, model interface{}, conditionOrId interface{}) error {
	return s.firstWithTransaction(context.Background(), tx, model, conditionOrId)
}

func (s PostgreSQLConnector) firstWithTransaction(ctx context.Context, tx *sql.Tx, model interface{}, conditionOrId interface{}) error {
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
	rows, err := s.doQueryInTransaction(ctx, tx, &queryProps)
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

func (s PostgreSQLConnector) AllWithTransactionAndContext(ctx context.Context, tx *sql.Tx, models interface{}, queryProps *DatabaseQuery) error {
	return s.allWithTransaction(ctx, tx, models, queryProps)
}

func (s PostgreSQLConnector) AllWithTransaction(tx *sql.Tx, models interface{}, queryProps *DatabaseQuery) error {
	return s.allWithTransaction(context.Background(), tx, models, queryProps)
}

func (s PostgreSQLConnector) AllWithContext(ctx context.Context, models interface{}, queryProps *DatabaseQuery) error {
	return s.all(ctx, models, queryProps)
}

func (s PostgreSQLConnector) All(models interface{}, queryProps *DatabaseQuery) error {
	return s.all(context.Background(), models, queryProps)
}

func (s PostgreSQLConnector) all(ctx context.Context, models interface{}, queryProps *DatabaseQuery) error {
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

func (s PostgreSQLConnector) allWithTransaction(ctx context.Context, tx *sql.Tx, models interface{}, queryProps *DatabaseQuery) error {
	// Ensure models is a pointer to a slice
	val := reflect.ValueOf(models)
	if val.Kind() != reflect.Ptr || val.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("error handling %s: models must be a pointer to a slice", val.Type())
	}

	if queryProps.Table == "" && queryProps.Model != nil {
		queryProps.Table = getTableNameFromModel(s.TablePrefix, queryProps.Model)
	}
	fieldMap := parseTags(queryProps.Model, &queryProps.fields)
	rows, err := s.doQueryInTransaction(ctx, tx, queryProps)
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

func (s PostgreSQLConnector) DeleteWithContext(ctx context.Context, model interface{}, condition ...Condition) (int64, error) {
	return s.delete(ctx, model, condition...)
}

func (s PostgreSQLConnector) Delete(model interface{}, condition ...Condition) (int64, error) {
	return s.delete(context.Background(), model, condition...)
}

func (s PostgreSQLConnector) delete(ctx context.Context, model interface{}, condition ...Condition) (int64, error) {
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
	stmt, err := db.PrepareContext(ctx, sql)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	// Execute the delete statement
	result, err := stmt.Exec(args...)
	if err != nil {
		return 0, err
	}
	affectedRows, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affectedRows, nil
}

func (s PostgreSQLConnector) DeleteByIdWithContext(ctx context.Context, model interface{}, id interface{}) (int64, error) {
	return s.deleteById(ctx, model, id)
}

func (s PostgreSQLConnector) DeleteById(model interface{}, id interface{}) (int64, error) {
	return s.deleteById(context.Background(), model, id)
}

func (s PostgreSQLConnector) deleteById(ctx context.Context, model interface{}, id interface{}) (int64, error) {
	return s.DeleteWithContext(ctx, model, Condition{
		Field:    "id",
		Operator: "=",
		Value:    id,
	})
}

func (s PostgreSQLConnector) UpdateWithContext(ctx context.Context, model interface{}, conditionsOrNil interface{}) (int64, error) {
	return s.update(ctx, model, conditionsOrNil)
}

func (s PostgreSQLConnector) Update(model interface{}, conditionsOrNil interface{}) (int64, error) {
	return s.update(context.Background(), model, conditionsOrNil)
}

func (s PostgreSQLConnector) update(ctx context.Context, model interface{}, conditionsOrNil interface{}) (int64, error) {
	updateStmt := DatabaseUpdate{
		Table: getTableNameFromModel(s.TablePrefix, model),
	}
	if conditionsOrNil != nil {
		switch v := conditionsOrNil.(type) {
		case []Condition:
			updateStmt.Condition = append(updateStmt.Condition, v...)
		default:
			return 0, fmt.Errorf("conditionsOrNil must be a slice of Condition")
		}
	}
	parseTags(model, &updateStmt.Fields)
	val := reflect.ValueOf(model)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	t := val.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.Tag.Get("db_column") == "id" && len(updateStmt.Condition) == 0 {
			updateStmt.Condition = append(updateStmt.Condition, []Condition{
				{
					Field:    "id",
					Operator: "=",
					Value:    val.Field(i).Interface(),
				},
			}...)
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

func (s *PostgreSQLConnector) doQueryInTransaction(ctx context.Context, tx *sql.Tx, queryProps *DatabaseQuery) (rows *sql.Rows, err error) {
	var q string
	var args []interface{}
	if queryProps.AllowPagination || queryProps.AllowSearch {
		q, args = buildAdvancedQuery(queryProps)
	} else {
		q, args = buildQuery(queryProps)
	}
	// Perform a query
	rows, err = tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (s *PostgreSQLConnector) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return s.db.BeginTx(ctx, opts)
}

func (s *PostgreSQLConnector) CommitTx(tx *sql.Tx) error {
	return tx.Commit()
}

func (s *PostgreSQLConnector) RollbackTx(tx *sql.Tx) error {
	return tx.Rollback()
}

func (s *PostgreSQLConnector) InsertWithTransactionAndContext(ctx context.Context, tx *sql.Tx, model interface{}) error {
	insertStmt := DatabaseInsert{
		Table: getTableNameFromModel(s.TablePrefix, model),
	}
	parseTags(model, &insertStmt.Fields)
	q, args, err := buildInsertStmt(&insertStmt, model)
	if err != nil {
		return err
	}
	// Prepare the query
	stmt, err := tx.PrepareContext(ctx, q)
	if err != nil {
		return err
	}
	defer stmt.Close()
	// Execute the query
	_, err = stmt.ExecContext(ctx, args...)
	return err
}
func (s *PostgreSQLConnector) InsertWithTransaction(tx *sql.Tx, model interface{}) error {
	ctx := context.Background()
	insertStmt := DatabaseInsert{
		Table: getTableNameFromModel(s.TablePrefix, model),
	}
	parseTags(model, &insertStmt.Fields)
	q, args, err := buildInsertStmt(&insertStmt, model)
	if err != nil {
		return err
	}
	// Prepare the query
	stmt, err := tx.PrepareContext(ctx, q)
	if err != nil {
		return err
	}
	defer stmt.Close()
	// Execute the query
	_, err = stmt.ExecContext(ctx, args...)
	return err
}

func (s *PostgreSQLConnector) UpdateWithTransactionAndContext(ctx context.Context, tx *sql.Tx, model interface{}) (int64, error) {
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
	// Prepare the query
	stmt, err := tx.PrepareContext(ctx, q)
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

func (s *PostgreSQLConnector) UpdateWithTransaction(tx *sql.Tx, model interface{}) (int64, error) {
	ctx := context.Background()
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
	// Prepare the query
	stmt, err := tx.PrepareContext(ctx, q)
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

func (s *PostgreSQLConnector) DeleteWithTransactionAndContext(ctx context.Context, tx *sql.Tx, model interface{}, condition ...Condition) error {
	deleteStmt := DatabaseDelete{
		Table:     getTableNameFromModel(s.TablePrefix, model),
		Condition: condition,
	}
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
	stmt, err := tx.PrepareContext(ctx, sql)
	if err != nil {
		return err
	}
	defer stmt.Close()

	// Execute the delete statement
	_, err = stmt.Exec(args...)
	if err != nil {
		return err
	}
	return nil
}

func (s *PostgreSQLConnector) DeleteWithTransaction(tx *sql.Tx, model interface{}, condition ...Condition) error {
	ctx := context.Background()
	deleteStmt := DatabaseDelete{
		Table:     getTableNameFromModel(s.TablePrefix, model),
		Condition: condition,
	}
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
	stmt, err := tx.PrepareContext(ctx, sql)
	if err != nil {
		return err
	}
	defer stmt.Close()

	// Execute the delete statement
	_, err = stmt.Exec(args...)
	if err != nil {
		return err
	}
	return nil
}
