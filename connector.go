package db

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	_ "github.com/lib/pq"
)

const defaultTablePrefix = DefaultTablePrefix

// Helper function to process options
func processOptions(opts []Option) *Config {
	config := &Config{ctx: context.Background()}
	for _, opt := range opts {
		opt(config)
	}
	return config
}

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

func (s PostgreSQLConnector) insert(ctx context.Context, model interface{}) (err error) {
	return s.insertWithTx(ctx, nil, model)
}

func (s PostgreSQLConnector) insertWithTx(ctx context.Context, tx *sql.Tx, model interface{}) (err error) {
	insertStmt := DatabaseInsert{
		Table: getTableNameFromModel(s.TablePrefix, model),
	}
	parseTags(model, &insertStmt.Fields)
	q, args, err := buildInsertStmt(&insertStmt, model)
	if err != nil {
		return
	}

	// Prepare the query
	var stmt *sql.Stmt
	if tx != nil {
		stmt, err = tx.PrepareContext(ctx, q)
	} else {
		db := s.GetConnection()
		stmt, err = db.PrepareContext(ctx, q)
	}
	if err != nil {
		return
	}
	defer stmt.Close()
	// Execute the query
	_, err = stmt.ExecContext(ctx, args...)
	return
}

func (s PostgreSQLConnector) CustomMutate(ctx context.Context, transactionOrNil *sql.Tx, query string, args ...interface{}) (result *sql.Result, err error) {
	var stmt *sql.Stmt
	if transactionOrNil != nil {
		stmt, err := transactionOrNil.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		defer stmt.Close()
	} else {
		db := s.GetConnection()
		// Prepare the query
		stmt, err := db.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		defer stmt.Close()
	}
	// Execute the query
	res, err := stmt.ExecContext(ctx, args...)
	return &res, err
}

func (s PostgreSQLConnector) CustomQuery(ctx context.Context, transactionOrNil *sql.Tx, query string, args ...interface{}) (rows *sql.Rows, err error) {
	var stmt *sql.Stmt
	if transactionOrNil != nil {
		stmt, err := transactionOrNil.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		defer stmt.Close()
	} else {
		db := s.GetConnection()
		// Prepare the query
		stmt, err := db.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		defer stmt.Close()
	}
	// Perform a query
	rows, err = stmt.QueryContext(ctx, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
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
				Field:    DefaultIDField,
				Operator: "=",
				Value:    v,
			},
		}
	}
	var queryProps DatabaseQuery
	queryProps.Table = getTableNameFromModel(s.TablePrefix, model)
	queryProps.Conditions = condition
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
				Field:    DefaultIDField,
				Operator: "=",
				Value:    v,
			},
		}
	}
	var queryProps DatabaseQuery
	queryProps.Table = getTableNameFromModel(s.TablePrefix, model)
	queryProps.Conditions = condition
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

func (s PostgreSQLConnector) all(ctx context.Context, models interface{}, queryProps *DatabaseQuery) error {
	// Ensure models is a pointer to a slice
	val := reflect.ValueOf(models)
	if val.Kind() != reflect.Ptr || val.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("error handling %s: models must be a pointer to a slice", val.Type())
	}

	// Extract model type from slice
	sliceType := val.Elem().Type()
	elementType := sliceType.Elem()
	// Create a new instance of the element type
	modelInstance := reflect.New(elementType).Interface()

	if queryProps.Table == "" {
		queryProps.Table = getTableNameFromModel(s.TablePrefix, modelInstance)
	}
	fieldMap := parseTags(modelInstance, &queryProps.fields)
	rows, err := s.doQuery(ctx, queryProps)
	if err != nil {
		return fmt.Errorf("error querying database: %v", err)
	}
	defer rows.Close()
	columns, _ := rows.Columns()

	// scan rows into "models" slice
	for rows.Next() {
		scanArgs := make([]interface{}, len(columns))
		modelType := reflect.TypeOf(modelInstance)
		if modelType.Kind() == reflect.Ptr {
			modelType = modelType.Elem()
		}
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

	// Extract model type from slice
	sliceType := val.Elem().Type()
	elementType := sliceType.Elem()
	// Create a new instance of the element type
	modelInstance := reflect.New(elementType).Interface()

	if queryProps.Table == "" {
		queryProps.Table = getTableNameFromModel(s.TablePrefix, modelInstance)
	}
	fieldMap := parseTags(modelInstance, &queryProps.fields)
	rows, err := s.doQueryInTransaction(ctx, tx, queryProps)
	if err != nil {
		return fmt.Errorf("error querying database: %v", err)
	}
	defer rows.Close()
	columns, _ := rows.Columns()

	// scan rows into "models" slice
	for rows.Next() {
		scanArgs := make([]interface{}, len(columns))
		modelType := reflect.TypeOf(modelInstance)
		if modelType.Kind() == reflect.Ptr {
			modelType = modelType.Elem()
		}
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

func (s PostgreSQLConnector) Query(ctx context.Context, model interface{}, queryProps *DatabaseQuery) ([]interface{}, error) {
	if queryProps.Table == "" {
		queryProps.Table = getTableNameFromModel(s.TablePrefix, model)
	}
	fieldMap := parseTags(model, &queryProps.fields)
	rows, err := s.doQuery(ctx, queryProps)
	if err != nil {
		return nil, fmt.Errorf("error querying database: %v", err)
	}
	defer rows.Close()

	var results []interface{}
	columns, _ := rows.Columns()
	for rows.Next() {
		scanArgs := make([]interface{}, len(columns))
		val := reflect.New(reflect.TypeOf(model).Elem())
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

func (s PostgreSQLConnector) delete(ctx context.Context, model interface{}, condition ...Condition) (int64, error) {
	return s.deleteWithTx(ctx, nil, model, condition...)
}

func (s PostgreSQLConnector) deleteWithTx(ctx context.Context, tx *sql.Tx, model interface{}, condition ...Condition) (int64, error) {
	deleteStmt := DatabaseDelete{
		Table:      getTableNameFromModel(s.TablePrefix, model),
		Conditions: condition,
	}

	// Start the delete statement
	query := fmt.Sprintf("DELETE FROM %s", deleteStmt.Table)

	// Add the conditions
	var args []interface{}
	if len(deleteStmt.Conditions) > 0 {
		query += " WHERE "
		for i, condition := range deleteStmt.Conditions {
			if i > 0 {
				query += " AND "
			}
			if condition.Operator == "LIKE" || condition.Operator == "NOT LIKE" {
				query += fmt.Sprintf("%s %s $%d", condition.Field, condition.Operator, i+1)
			} else {
				switch condition.Value.(type) {
				case int, int32, int64, float32, float64:
					query += fmt.Sprintf("%s %s $%d", condition.Field, condition.Operator, i+1)
				default:
					query += fmt.Sprintf("%s %s $%d", condition.Field, condition.Operator, i+1)
				}
			}
			args = append(args, condition.Value)
		}
	}

	// Prepare the statement
	var stmt *sql.Stmt
	var err error
	if tx != nil {
		stmt, err = tx.PrepareContext(ctx, query)
	} else {
		db := s.GetConnection()
		stmt, err = db.PrepareContext(ctx, query)
	}
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

func (s PostgreSQLConnector) update(ctx context.Context, model interface{}, conditionsOrNil interface{}) (int64, error) {
	return s.updateWithTx(ctx, nil, model, conditionsOrNil)
}

func (s PostgreSQLConnector) updateWithTx(ctx context.Context, tx *sql.Tx, model interface{}, conditionsOrNil interface{}) (int64, error) {
	updateStmt := DatabaseUpdate{
		Table: getTableNameFromModel(s.TablePrefix, model),
	}
	if conditionsOrNil != nil {
		switch v := conditionsOrNil.(type) {
		case []Condition:
			updateStmt.Conditions = append(updateStmt.Conditions, v...)
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
		if field.Tag.Get(DBColumnTag) == DefaultIDField && len(updateStmt.Conditions) == 0 {
			updateStmt.Conditions = append(updateStmt.Conditions, []Condition{
				{
					Field:    DefaultIDField,
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

	// Prepare the query
	var stmt *sql.Stmt
	if tx != nil {
		stmt, err = tx.PrepareContext(ctx, q)
	} else {
		db := s.GetConnection()
		stmt, err = db.PrepareContext(ctx, q)
	}
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

func prefixColumns(tableName string, columns []string) []string {
	prefixedCols := make([]string, len(columns))
	for i, col := range columns {
		prefixedCols[i] = fmt.Sprintf("%s.%s", tableName, col)
	}
	return prefixedCols
}

func (s *PostgreSQLConnector) JoinWithContext(ctx context.Context, props *JoinProps) ([]map[string]interface{}, error) {
	return s.join(ctx, props)
}

func (s *PostgreSQLConnector) join(ctx context.Context, props *JoinProps) ([]map[string]interface{}, error) {
	mainTableName := getTableNameFromModel(s.TablePrefix, props.MainTableModel)
	joinTableName := getTableNameFromModel(s.TablePrefix, props.JoinTableModel)

	// Prefix columns with table names
	mainTableCols := prefixColumns(mainTableName, props.MainTableCols)
	joinTableCols := prefixColumns(joinTableName, props.JoinTableCols)

	// Build the SQL query
	query := fmt.Sprintf("SELECT %s, %s FROM %s JOIN %s ON %s",
		strings.Join(mainTableCols, ", "),
		strings.Join(joinTableCols, ", "),
		mainTableName,
		joinTableName,
		props.JoinCondition,
	)

	var args []interface{}
	if len(props.WhereConditions) > 0 {
		query += " WHERE "
		for i, condition := range props.WhereConditions {
			if i > 0 {
				query += " AND "
			}
			query += fmt.Sprintf("%s %s $%d", condition.Field, condition.Operator, i+1)
			args = append(args, condition.Value)
		}
	}

	db := s.GetConnection()
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("error executing join query: %v", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("error getting columns: %v", err)
	}

	// Prepare a slice to hold the results
	var results []map[string]interface{}

	// Iterate over the rows
	for rows.Next() {
		// Create a map to hold the row data
		rowData := make(map[string]interface{})
		// Create a slice to hold the values
		values := make([]interface{}, len(columns))
		// Create a slice to hold the pointers to the values
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Scan the row into the value pointers
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("error scanning row: %v", err)
		}

		// Populate the rowData map
		for i, col := range columns {
			rowData[col] = values[i]
		}

		// Append the rowData map to the results slice
		results = append(results, rowData)
	}

	return results, nil
}

// Simplified methods using options pattern

// InsertModel is a simplified insert method that accepts optional context and transaction
func (s PostgreSQLConnector) InsertModel(model interface{}, opts ...Option) error {
	config := processOptions(opts)
	if config.tx != nil {
		return s.insertWithTx(config.ctx, config.tx, model)
	}
	return s.insert(config.ctx, model)
}

// DeleteModel is a simplified delete method that accepts optional context and transaction
func (s PostgreSQLConnector) DeleteModel(model interface{}, conditions []Condition, opts ...Option) (int64, error) {
	config := processOptions(opts)
	if config.tx != nil {
		return s.deleteWithTx(config.ctx, config.tx, model, conditions...)
	}
	return s.delete(config.ctx, model, conditions...)
}

// UpdateModel is a simplified update method that accepts optional context and transaction
func (s PostgreSQLConnector) UpdateModel(model interface{}, conditions interface{}, opts ...Option) (int64, error) {
	config := processOptions(opts)
	if config.tx != nil {
		return s.updateWithTx(config.ctx, config.tx, model, conditions)
	}
	return s.update(config.ctx, model, conditions)
}

// FindFirst is a simplified method to find the first record matching conditions
func (s PostgreSQLConnector) FindFirst(model interface{}, conditionOrId interface{}, opts ...Option) error {
	config := processOptions(opts)
	if config.tx != nil {
		return s.firstWithTransaction(config.ctx, config.tx, model, conditionOrId)
	}
	return s.first(config.ctx, model, conditionOrId)
}

// FindAll is a simplified method to find all records matching the query
func (s PostgreSQLConnector) FindAll(models interface{}, queryProps *DatabaseQuery, opts ...Option) error {
	config := processOptions(opts)
	if config.tx != nil {
		return s.allWithTransaction(config.ctx, config.tx, models, queryProps)
	}
	return s.all(config.ctx, models, queryProps)
}
