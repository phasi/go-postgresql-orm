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

func (s *PostgreSQLConnector) MigrateTable(model interface{}) error {
	tableName := getTableNameFromModel(s.TablePrefix, model)
	columns, foreignKeys := getColumnsAndForeignKeysFromStructWithPrefix(model, s.TablePrefix)
	table := Table{Name: tableName, Columns: columns, ForeignKeys: foreignKeys}
	db := s.GetConnection()
	return _migrateTable(db, table)
}

// CreateTable creates a single table in the database for the given model
func (s *PostgreSQLConnector) CreateTable(model interface{}) error {
	tableName := getTableNameFromModel(s.TablePrefix, model)
	columns, foreignKeys := getColumnsAndForeignKeysFromStructWithPrefix(model, s.TablePrefix)
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

func (s *PostgreSQLConnector) MigrateTables(models ...interface{}) error {
	for _, model := range models {
		err := s.MigrateTable(model)
		if err != nil {
			return err
		}
	}
	return nil
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

func (s *PostgreSQLConnector) ListTables() ([]string, error) {
	db := s.GetConnection()
	return listTables(db)
}

// ListColumns lists the columns of a table given a model or table name (string)
func (s *PostgreSQLConnector) ListColumns(table interface{}) (Columns, error) {
	tableName := ""
	var ok bool
	tableName, ok = table.(string)
	if !ok {
		if tableName == "" {
			tableName = getTableNameFromModel(s.TablePrefix, table)
		}
	}
	db := s.GetConnection()
	return listColumns(db, tableName)
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
	stmt, err := prepareStatement(ctx, tx, s.GetConnection(), q)
	if err != nil {
		return
	}
	defer stmt.Close()
	// Execute the query
	_, err = stmt.ExecContext(ctx, args...)
	return
}

func (s PostgreSQLConnector) CustomMutate(ctx context.Context, transactionOrNil *sql.Tx, query string, args ...interface{}) (result *sql.Result, err error) {
	stmt, err := prepareStatement(ctx, transactionOrNil, s.GetConnection(), query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	// Execute the query
	res, err := stmt.ExecContext(ctx, args...)
	return &res, err
}

func (s PostgreSQLConnector) CustomQuery(ctx context.Context, transactionOrNil *sql.Tx, query string, args ...interface{}) (rows *sql.Rows, err error) {
	stmt, err := prepareStatement(ctx, transactionOrNil, s.GetConnection(), query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	// Perform a query
	rows, err = stmt.QueryContext(ctx, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (s PostgreSQLConnector) first(ctx context.Context, tx *sql.Tx, model interface{}, conditionOrId interface{}) error {
	if conditionOrId == nil {
		return fmt.Errorf("conditionOrId cannot be nil")
	}
	var condition []Condition
	switch v := conditionOrId.(type) {
	case []Condition:
		condition = v
	default:
		condition = createPrimaryKeyCondition(model, v)
	}
	var queryProps DatabaseQuery
	queryProps.Table = getTableNameFromModel(s.TablePrefix, model)
	queryProps.Conditions = condition
	queryProps.Limit = 1
	fieldMap := parseTags(model, &queryProps.fields)
	rows, err := s.executeQuery(ctx, tx, &queryProps)
	if err != nil {
		return fmt.Errorf("error querying database: %v", err)
	}
	defer rows.Close()
	if rows.Next() {
		columns, _ := rows.Columns()
		val := reflect.ValueOf(model).Elem()
		scanArgs := scanRowToModel(columns, fieldMap, val)
		err = rows.Scan(scanArgs...)
		if err != nil {
			return fmt.Errorf("error scanning row: %v", err)
		}
	}
	return nil
}

func (s PostgreSQLConnector) all(ctx context.Context, tx *sql.Tx, models interface{}, queryProps *DatabaseQuery) error {
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
	rows, err := s.executeQuery(ctx, tx, queryProps)
	if err != nil {
		return fmt.Errorf("error querying database: %v", err)
	}
	defer rows.Close()
	columns, _ := rows.Columns()

	// scan rows into "models" slice
	for rows.Next() {
		modelType := reflect.TypeOf(modelInstance)
		if modelType.Kind() == reflect.Ptr {
			modelType = modelType.Elem()
		}
		modelVal := reflect.New(modelType)
		scanArgs := scanRowToModel(columns, fieldMap, modelVal.Elem())
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
	rows, err := s.executeQuery(ctx, nil, queryProps)
	if err != nil {
		return nil, fmt.Errorf("error querying database: %v", err)
	}
	defer rows.Close()

	var results []interface{}
	columns, _ := rows.Columns()
	for rows.Next() {
		val := reflect.New(reflect.TypeOf(model).Elem())
		scanArgs := scanRowToModel(columns, fieldMap, val.Elem())
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, fmt.Errorf("error scanning row: %v", err)
		}
		results = append(results, val.Interface())
	}
	return results, nil
}

func (s PostgreSQLConnector) deleteWithTx(ctx context.Context, tx *sql.Tx, model interface{}, condition ...Condition) (int64, error) {
	deleteStmt := DatabaseDelete{
		Table:      getTableNameFromModel(s.TablePrefix, model),
		Conditions: condition,
	}

	// Use QueryBuilder for consistent DELETE query building
	qb := NewQueryBuilder()
	qb.DeleteFrom(deleteStmt.Table)

	// Add conditions using centralized logic
	for _, cond := range deleteStmt.Conditions {
		qb.Where(cond.Field, cond.Operator, cond.Value)
	}

	query, args, err := qb.Build()
	if err != nil {
		return 0, fmt.Errorf("error building DELETE query: %v", err)
	}

	// Prepare the statement
	stmt, err := prepareStatement(ctx, tx, s.GetConnection(), query)
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
		if isPrimaryKeyField(field) && len(updateStmt.Conditions) == 0 {
			gpoField := parseGPOTag(field)
			if gpoField != nil {
				updateStmt.Conditions = append(updateStmt.Conditions, []Condition{
					{
						Field:    gpoField.ColumnName,
						Operator: "=",
						Value:    val.Field(i).Interface(),
					},
				}...)
			}
			break
		}
	}
	q, args, err := buildUpdateStmt(&updateStmt, model)
	if err != nil {
		return 0, err
	}

	// Prepare the query
	stmt, err := prepareStatement(ctx, tx, s.GetConnection(), q)
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

// executeQuery executes a query with optional transaction support
func (s *PostgreSQLConnector) executeQuery(ctx context.Context, tx *sql.Tx, queryProps *DatabaseQuery) (rows *sql.Rows, err error) {
	var q string
	var args []interface{}
	if queryProps.AllowPagination || queryProps.AllowSearch {
		q, args = buildAdvancedQuery(queryProps)
	} else {
		q, args = buildQuery(queryProps)
	}

	if tx != nil {
		return tx.QueryContext(ctx, q, args...)
	}

	db := s.GetConnection()
	return db.QueryContext(ctx, q, args...)
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

func (s *PostgreSQLConnector) join(ctx context.Context, props *JoinProps) ([]map[string]interface{}, error) {
	// Validate join type
	if props.JoinType == "" {
		return nil, fmt.Errorf("join type is required")
	}

	mainTableName := getTableNameFromModel(s.TablePrefix, props.MainTableModel)
	joinTableName := getTableNameFromModel(s.TablePrefix, props.JoinTableModel)

	// Build column selections with aliases to preserve table context
	var selectParts []string

	// Add main table columns with aliases
	for _, col := range props.MainTableCols {
		selectParts = append(selectParts, fmt.Sprintf("%s.%s AS \"%s.%s\"", mainTableName, col, mainTableName, col))
	}

	// Add join table columns with aliases
	for _, col := range props.JoinTableCols {
		selectParts = append(selectParts, fmt.Sprintf("%s.%s AS \"%s.%s\"", joinTableName, col, joinTableName, col))
	}

	// Build the SQL query with the specified join type
	query := fmt.Sprintf("SELECT %s FROM %s %s %s ON %s",
		strings.Join(selectParts, ", "),
		mainTableName,
		string(props.JoinType),
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
			val := values[i]
			// Convert byte arrays (common for UUIDs) to strings
			if byteVal, ok := val.([]byte); ok {
				val = string(byteVal)
			}
			rowData[col] = val
		}

		// Append the rowData map to the results slice
		results = append(results, rowData)
	}

	return results, nil
}

// joinIntoStruct performs a join operation and scans results into a struct slice
func (s *PostgreSQLConnector) joinIntoStruct(ctx context.Context, props *JoinResult) error {
	// Validate join type
	if props.JoinType == "" {
		return fmt.Errorf("join type is required")
	}

	// Ensure ResultModel is a pointer to a slice
	val := reflect.ValueOf(props.ResultModel)
	if val.Kind() != reflect.Ptr || val.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("ResultModel must be a pointer to a slice")
	}

	// Extract element type from slice
	sliceType := val.Elem().Type()
	elementType := sliceType.Elem()

	// Create a new instance of the element type to extract field information
	modelInstance := reflect.New(elementType).Interface()

	mainTableName := getTableNameFromModel(s.TablePrefix, props.MainTableModel)
	joinTableName := getTableNameFromModel(s.TablePrefix, props.JoinTableModel)

	// Parse tags from the result model to get field mapping
	var fields Fields
	fieldMap := parseTags(modelInstance, &fields)

	// Build column selections based on struct tags and custom mappings
	var selectParts []string

	if props.ColumnMappings != nil && len(props.ColumnMappings) > 0 {
		// Use explicit column mappings
		for tableColumn, structTag := range props.ColumnMappings {
			selectParts = append(selectParts, fmt.Sprintf("%s AS %s", tableColumn, structTag))
		}
	} else {
		// Auto-build from struct fields and table models
		var mainFields, joinFields Fields
		parseTags(props.MainTableModel, &mainFields)
		parseTags(props.JoinTableModel, &joinFields)

		// For each field in the result struct, try to map it to a table column
		for _, field := range fields {
			// Check if this field exists in main table
			if contains(mainFields, field) {
				selectParts = append(selectParts, fmt.Sprintf("%s.%s", mainTableName, field))
			} else if contains(joinFields, field) {
				selectParts = append(selectParts, fmt.Sprintf("%s.%s", joinTableName, field))
			}
		}
	}

	// Build the SQL query with the specified join type
	query := fmt.Sprintf("SELECT %s FROM %s %s %s ON %s",
		strings.Join(selectParts, ", "),
		mainTableName,
		string(props.JoinType),
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
		return fmt.Errorf("error executing join query: %v", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("error getting columns: %v", err)
	}

	// Scan rows into struct slice
	for rows.Next() {
		// Create a new instance of the element type
		newElement := reflect.New(elementType)
		elementVal := newElement.Elem()

		// Prepare scan arguments
		scanArgs := scanRowToModel(columns, fieldMap, elementVal)

		// Scan the row into the struct
		if err := rows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("error scanning row: %v", err)
		}

		// Append the new element to the slice
		val.Elem().Set(reflect.Append(val.Elem(), elementVal))
	}

	return nil
}

// Helper function to check if a slice contains a string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// InsertModel inserts a model into the database, accepting optional context and transaction
func (s PostgreSQLConnector) InsertModel(model interface{}, opts ...Option) error {
	config := processOptions(opts)
	return s.insertWithTx(config.ctx, config.tx, model)
}

// DeleteModel deletes a model from the database, accepting optional context and transaction
func (s PostgreSQLConnector) DeleteModel(model interface{}, conditions []Condition, opts ...Option) (int64, error) {
	config := processOptions(opts)
	return s.deleteWithTx(config.ctx, config.tx, model, conditions...)
}

// UpdateModel updates a model in the database, accepting optional context and transaction
func (s PostgreSQLConnector) UpdateModel(model interface{}, conditions interface{}, opts ...Option) (int64, error) {
	config := processOptions(opts)
	return s.updateWithTx(config.ctx, config.tx, model, conditions)
}

// FindFirst finds the first record matching the condition or primary key, accepting optional context and transaction
func (s PostgreSQLConnector) FindFirst(model interface{}, conditionOrId interface{}, opts ...Option) error {
	config := processOptions(opts)
	return s.first(config.ctx, config.tx, model, conditionOrId)
}

// FindAll finds all records matching the query properties, accepting optional context and transaction
func (s PostgreSQLConnector) FindAll(models interface{}, queryProps *DatabaseQuery, opts ...Option) error {
	config := processOptions(opts)
	return s.all(config.ctx, config.tx, models, queryProps)
}

// LeftJoinWithContext performs a LEFT JOIN between two tables
func (s *PostgreSQLConnector) LeftJoinWithContext(ctx context.Context, props *JoinProps) ([]map[string]interface{}, error) {
	props.JoinType = LeftJoin
	return s.join(ctx, props)
}

// RightJoinWithContext performs a RIGHT JOIN between two tables
func (s *PostgreSQLConnector) RightJoinWithContext(ctx context.Context, props *JoinProps) ([]map[string]interface{}, error) {
	props.JoinType = RightJoin
	return s.join(ctx, props)
}

// FullJoinWithContext performs a FULL OUTER JOIN between two tables
func (s *PostgreSQLConnector) FullJoinWithContext(ctx context.Context, props *JoinProps) ([]map[string]interface{}, error) {
	props.JoinType = FullJoin
	return s.join(ctx, props)
}

// InnerJoinWithContext performs an INNER JOIN between two tables
func (s *PostgreSQLConnector) InnerJoinWithContext(ctx context.Context, props *JoinProps) ([]map[string]interface{}, error) {
	props.JoinType = InnerJoin
	return s.join(ctx, props)
}

// LeftJoinIntoStruct performs a LEFT JOIN and scans results into a struct slice
func (s *PostgreSQLConnector) LeftJoinIntoStruct(ctx context.Context, props *JoinResult) error {
	props.JoinType = LeftJoin
	return s.joinIntoStruct(ctx, props)
}

// RightJoinIntoStruct performs a RIGHT JOIN and scans results into a struct slice
func (s *PostgreSQLConnector) RightJoinIntoStruct(ctx context.Context, props *JoinResult) error {
	props.JoinType = RightJoin
	return s.joinIntoStruct(ctx, props)
}

// FullJoinIntoStruct performs a FULL OUTER JOIN and scans results into a struct slice
func (s *PostgreSQLConnector) FullJoinIntoStruct(ctx context.Context, props *JoinResult) error {
	props.JoinType = FullJoin
	return s.joinIntoStruct(ctx, props)
}

// InnerJoinIntoStruct performs an INNER JOIN and scans results into a struct slice
func (s *PostgreSQLConnector) InnerJoinIntoStruct(ctx context.Context, props *JoinResult) error {
	props.JoinType = InnerJoin
	return s.joinIntoStruct(ctx, props)
}
