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

func parseTags(model interface{}, fields *Fields) FieldMap {
	val := reflect.ValueOf(model)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	fieldMap := make(FieldMap)
	for i := 0; i < val.NumField(); i++ {
		typeField := val.Type().Field(i)
		if gpoField := parseGPOTag(typeField); gpoField != nil {
			*fields = append(*fields, gpoField.ColumnName)
			fieldMap[gpoField.ColumnName] = typeField.Name
		}
	}
	return fieldMap
}

// parseGPOTag parses the gpo tag and returns GPOField information
func parseGPOTag(field reflect.StructField) *GPOField {
	tag, ok := field.Tag.Lookup(GPOTag)
	if !ok {
		return nil
	}

	parts := strings.Split(tag, ",")
	if len(parts) == 0 {
		return nil
	}

	gpoField := &GPOField{
		ColumnName: strings.TrimSpace(parts[0]),
	}

	// Parse options
	for i := 1; i < len(parts); i++ {
		option := strings.TrimSpace(parts[i])

		if option == "pk" {
			gpoField.IsPrimaryKey = true
		} else if option == "unique" {
			gpoField.IsUnique = true
		} else if option == "nullable" {
			gpoField.IsNullable = true
		} else if strings.HasPrefix(option, "length(") && strings.HasSuffix(option, ")") {
			// Parse length(50)
			lengthStr := option[7 : len(option)-1] // Remove "length(" and ")"
			if length, err := strconv.Atoi(lengthStr); err == nil {
				gpoField.Length = length
			}
		} else if strings.HasPrefix(option, "fk(") && strings.HasSuffix(option, ")") {
			// Parse fk(table:column) or fk(table:column,cascade)
			fkContent := option[3 : len(option)-1] // Remove "fk(" and ")"
			fkParts := strings.Split(fkContent, ",")

			if len(fkParts) >= 1 {
				// Parse table:column
				tableColumn := strings.TrimSpace(fkParts[0])
				if colonIdx := strings.Index(tableColumn, ":"); colonIdx != -1 {
					gpoField.ForeignKey = &ForeignKeyInfo{
						Table:  strings.TrimSpace(tableColumn[:colonIdx]),
						Column: strings.TrimSpace(tableColumn[colonIdx+1:]),
					}

					// Parse onDelete option if present
					if len(fkParts) >= 2 {
						gpoField.ForeignKey.OnDelete = strings.TrimSpace(fkParts[1])
					}
				}
			}
		}
	}

	return gpoField
}

func convertGoTypeToPostgresType(goType string, length int) string {
	// Convert Go type to Postgres type
	switch goType {
	case "string":
		if length > 0 && length <= 255 {
			return fmt.Sprintf("VARCHAR(%d)", length)
		} else if length > 255 {
			// If the length is greater than 255, use TEXT
			return "TEXT"
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
	case "time.Duration":
		return "BIGINT"
	default:
		if length > 0 {
			return fmt.Sprintf("VARCHAR(%d)", length)
		}
		return "VARCHAR(255)"
	}
}

func getColumnsAndForeignKeysFromStructWithPrefix(s interface{}, tablePrefix string) ([]Column, []ForeignKey) {
	t := reflect.TypeOf(s)

	// If the type is a pointer, get the element type
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	var columns []Column
	var foreignKeys []ForeignKey

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		gpoField := parseGPOTag(field)

		if gpoField != nil {
			columnType := convertGoTypeToPostgresType(field.Type.Name(), gpoField.Length)

			columns = append(columns, Column{
				Name:       gpoField.ColumnName,
				Type:       columnType,
				PrimaryKey: gpoField.IsPrimaryKey,
				Unique:     gpoField.IsUnique,
				Null:       gpoField.IsNullable,
				Length:     gpoField.Length,
			})

			// Handle foreign key
			if gpoField.ForeignKey != nil {
				// Add table prefix to the foreign key reference
				referencedTable := tablePrefix + gpoField.ForeignKey.Table
				references := fmt.Sprintf("%s(%s)", referencedTable, gpoField.ForeignKey.Column)

				foreignKey := ForeignKey{
					ColumnName: gpoField.ColumnName,
					References: references,
				}

				if gpoField.ForeignKey.OnDelete != "" {
					foreignKey.OnDelete = gpoField.ForeignKey.OnDelete
				}

				foreignKeys = append(foreignKeys, foreignKey)
			}
		}
	}

	// Check if we have a primary key column defined
	hasPrimaryKey := false
	for _, column := range columns {
		if column.PrimaryKey {
			hasPrimaryKey = true
			break
		}
	}

	// If no primary key is defined, add the default id column
	if !hasPrimaryKey {
		columns = append([]Column{{Name: DefaultIDField, Type: "UUID", PrimaryKey: true, Unique: false, Null: false, Length: 0}}, columns...)
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

func tableExists(db *sql.DB, tableName string) (bool, error) {
	var exists bool
	query := "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1)"
	err := db.QueryRow(query, tableName).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func _alterTable(db *sql.DB, table Table) error {
	// Get existing columns from the database
	existingColumns := make(map[string]Column)
	rows, err := db.Query(fmt.Sprintf("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '%s'", table.Name))
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var colName, dataType, isNullable string
		if err := rows.Scan(&colName, &dataType, &isNullable); err != nil {
			return err
		}
		existingColumns[colName] = Column{
			Name: colName,
			Type: dataType,
			Null: isNullable == "YES",
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// Compare and alter table as needed
	for _, column := range table.Columns {
		if existingCol, exists := existingColumns[column.Name]; !exists {
			// Column does not exist, add it
			nullText := "NOT NULL"
			if column.Null {
				nullText = "NULL"
			}
			uniqueText := ""
			if column.Unique {
				uniqueText = "UNIQUE"
			}
			sql := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s %s %s", table.Name, column.Name, column.Type, nullText, uniqueText)
			if _, err := db.Exec(sql); err != nil {
				return err
			}
		} else {
			// Column exists, check for type or nullability changes
			if existingCol.Type != column.Type {
				sql := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s", table.Name, column.Name, column.Type)
				if _, err := db.Exec(sql); err != nil {
					return err
				}
			}
			if existingCol.Null != column.Null {
				nullConstraint := "NOT NULL"
				if column.Null {
					nullConstraint = "NULL"
				}
				sql := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET %s", table.Name, column.Name, nullConstraint)
				if _, err := db.Exec(sql); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func _migrateTable(db *sql.DB, table Table) error {
	// Check if the table exists
	exists, err := tableExists(db, table.Name)
	if err != nil {
		return err
	}

	if !exists {
		return _createTable(db, table)
	}
	return _alterTable(db, table)
}

func listColumns(db *sql.DB, tableName string) (Columns, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '%s'", tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns Columns
	for rows.Next() {
		var col Column
		var isNullable string
		if err := rows.Scan(&col.Name, &col.Type, &isNullable); err != nil {
			return nil, err
		}
		col.Null = isNullable == "YES"
		columns = append(columns, col)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return columns, nil
}

func listTables(db *sql.DB) ([]string, error) {
	rows, err := db.Query("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tables, nil
}

func _createTable(db *sql.DB, table Table) error {
	if table.Name == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	// Start the create table statement
	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", table.Name)

	// Add columns to the table
	for _, column := range table.Columns {
		nullText := "NOT NULL"
		if column.Null {
			nullText = "NULL"
		}
		uniqueText := ""
		if column.Unique {
			uniqueText = "UNIQUE"
		}
		pkText := ""
		if column.PrimaryKey {
			pkText = "PRIMARY KEY"
		}
		sql += fmt.Sprintf("%s %s %s %s %s,", column.Name, column.Type, nullText, uniqueText, pkText)
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

func buildQuery(params *DatabaseQuery) (string, []interface{}) {
	// Use QueryBuilder for consistent query building
	qb := NewQueryBuilder()
	qb.Select(params.fields.String()...).From(params.Table)

	// Add conditions
	for _, condition := range params.Conditions {
		qb.Where(condition.Field, condition.Operator, condition.Value)
	}

	// Add ordering
	if params.OrderBy != "" {
		if params.Descending {
			qb.OrderByDesc(params.OrderBy)
		} else {
			qb.OrderByAsc(params.OrderBy)
		}
	}

	// Add limit
	if params.Limit > 0 {
		qb.Limit(params.Limit)
	}

	query, args, _ := qb.Build()
	return query, args
}

func ParseQueryParamsFromRequest(r *http.Request, query *DatabaseQuery) {
	query.Limit = 10
	query.Offset = 0
	query.Descending = false
	if limit := r.URL.Query().Get("limit"); limit != "" {
		query.Limit, _ = strconv.Atoi(limit)
	}
	if offset := r.URL.Query().Get("offset"); offset != "" {
		query.Offset, _ = strconv.Atoi(offset)
	}
	if orderBy := r.URL.Query().Get("order_by"); orderBy != "" {
		query.OrderBy = orderBy
	}
	if order := r.URL.Query().Get("order"); order == "desc" {
		query.Descending = true
	}
	if searchText := r.URL.Query().Get("search"); searchText != "" {
		query.SearchText = searchText
	}

}

func buildAdvancedQuery(params *DatabaseQuery) (string, []interface{}) {
	// Use QueryBuilder for consistent query building with search
	qb := NewQueryBuilder()
	qb.Select(params.fields.String()...).From(params.Table)

	// Add conditions
	for _, condition := range params.Conditions {
		qb.Where(condition.Field, condition.Operator, condition.Value)
	}

	// Add search functionality
	if len(params.SearchFields) > 0 && params.SearchText != "" {
		qb.Search(params.SearchFields.String(), params.SearchText)
	}

	// Add ordering
	if params.OrderBy != "" {
		if params.Descending {
			qb.OrderByDesc(params.OrderBy)
		} else {
			qb.OrderByAsc(params.OrderBy)
		}
	}

	// Add limit (default to 10 if not specified)
	if params.Limit > 0 {
		qb.Limit(params.Limit)
	} else {
		qb.Limit(10)
	}

	// Add offset
	if params.Offset > 0 {
		qb.Offset(params.Offset)
	}

	query, args, _ := qb.Build()
	return query, args
}

func buildInsertStmt(params *DatabaseInsert, model interface{}) (string, []interface{}, error) {
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
			if gpoField := parseGPOTag(field); gpoField != nil && gpoField.ColumnName == dbColumnName {
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

func buildUpdateStmt(params *DatabaseUpdate, model interface{}) (string, []interface{}, error) {
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
		gpoField := parseGPOTag(field)
		if gpoField == nil || gpoField.IsPrimaryKey {
			continue
		}
		query += fmt.Sprintf("%s = $%d, ", gpoField.ColumnName, len(args)+1)
		args = append(args, val.Field(i).Interface())
	}
	query = strings.TrimSuffix(query, ", ")

	// Use centralized condition building
	if len(params.Conditions) > 0 {
		whereClause, whereArgs := buildConditions(params.Conditions, args)
		if whereClause != "" {
			query += " WHERE " + whereClause
			args = whereArgs
		}
	}
	return query, args, nil
}

// buildConditions builds WHERE conditions from a slice of Condition structs with centralized IN/NOT IN handling
func buildConditions(conditions []Condition, existingArgs []interface{}) (string, []interface{}) {
	if len(conditions) == 0 {
		return "", existingArgs
	}

	var conditionParts []string
	args := existingArgs

	for _, condition := range conditions {
		if condition.Operator == "IN" || condition.Operator == "NOT IN" {
			// Handle IN/NOT IN with reflection for any slice type
			v := reflect.ValueOf(condition.Value)
			if v.Kind() == reflect.Slice {
				placeholders := make([]string, v.Len())
				for i := 0; i < v.Len(); i++ {
					placeholders[i] = fmt.Sprintf("$%d", len(args)+1)
					args = append(args, v.Index(i).Interface())
				}
				conditionParts = append(conditionParts, fmt.Sprintf("%s %s (%s)",
					condition.Field, condition.Operator, strings.Join(placeholders, ",")))
			} else {
				// Single value, treat as equals
				conditionParts = append(conditionParts, fmt.Sprintf("%s = $%d", condition.Field, len(args)+1))
				args = append(args, condition.Value)
			}
		} else if condition.Operator == "LIKE" || condition.Operator == "NOT LIKE" {
			conditionParts = append(conditionParts, fmt.Sprintf("%s %s $%d", condition.Field, condition.Operator, len(args)+1))
			args = append(args, "%"+condition.Value.(string)+"%")
		} else {
			conditionParts = append(conditionParts, fmt.Sprintf("%s %s $%d", condition.Field, condition.Operator, len(args)+1))
			args = append(args, condition.Value)
		}
	}

	return strings.Join(conditionParts, " AND "), args
}

// buildConditionsWithSearch builds WHERE conditions including search functionality
func buildConditionsWithSearch(conditions []Condition, searchFields []string, searchText string, existingArgs []interface{}) (string, []interface{}) {
	var whereParts []string
	args := existingArgs

	// Add regular conditions
	if len(conditions) > 0 {
		whereClause, whereArgs := buildConditions(conditions, args)
		if whereClause != "" {
			whereParts = append(whereParts, whereClause)
			args = whereArgs
		}
	}

	// Add search conditions
	if len(searchFields) > 0 && searchText != "" {
		var searchParts []string
		for _, field := range searchFields {
			searchParts = append(searchParts, fmt.Sprintf("%s LIKE $%d", field, len(args)+1))
			args = append(args, "%"+searchText+"%")
		}
		if len(searchParts) > 0 {
			whereParts = append(whereParts, "("+strings.Join(searchParts, " OR ")+")")
		}
	}

	if len(whereParts) == 0 {
		return "", args
	}

	return strings.Join(whereParts, " AND "), args
}

// QueryBuilder provides a fluent interface for building ALL SQL queries
type QueryBuilder struct {
	queryType    string
	table        string
	fields       []string
	joins        []string
	conditions   []Condition
	orderBy      []string
	groupBy      []string
	having       []string
	limit        int
	offset       int
	values       map[string]interface{}
	updateModel  interface{}
	insertModel  interface{}
	searchText   string
	searchFields []string
}

// NewQueryBuilder creates a new QueryBuilder instance
func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{
		fields:     []string{},
		joins:      []string{},
		conditions: []Condition{},
		orderBy:    []string{},
		groupBy:    []string{},
		having:     []string{},
		values:     make(map[string]interface{}),
	}
}

// SELECT operations
func (qb *QueryBuilder) Select(fields ...string) *QueryBuilder {
	qb.queryType = "SELECT"
	if len(fields) == 0 {
		qb.fields = []string{"*"}
	} else {
		qb.fields = fields
	}
	return qb
}

func (qb *QueryBuilder) From(table string) *QueryBuilder {
	qb.table = table
	return qb
}

// JOIN operations
func (qb *QueryBuilder) Join(table, condition string) *QueryBuilder {
	qb.joins = append(qb.joins, fmt.Sprintf("JOIN %s ON %s", table, condition))
	return qb
}

func (qb *QueryBuilder) LeftJoin(table, condition string) *QueryBuilder {
	qb.joins = append(qb.joins, fmt.Sprintf("LEFT JOIN %s ON %s", table, condition))
	return qb
}

func (qb *QueryBuilder) RightJoin(table, condition string) *QueryBuilder {
	qb.joins = append(qb.joins, fmt.Sprintf("RIGHT JOIN %s ON %s", table, condition))
	return qb
}

func (qb *QueryBuilder) FullJoin(table, condition string) *QueryBuilder {
	qb.joins = append(qb.joins, fmt.Sprintf("FULL OUTER JOIN %s ON %s", table, condition))
	return qb
}

// WHERE conditions using centralized buildConditions
func (qb *QueryBuilder) Where(field, operator string, value interface{}) *QueryBuilder {
	qb.conditions = append(qb.conditions, Condition{
		Field:    field,
		Operator: operator,
		Value:    value,
	})
	return qb
}

func (qb *QueryBuilder) WhereIn(field string, values interface{}) *QueryBuilder {
	qb.conditions = append(qb.conditions, Condition{
		Field:    field,
		Operator: "IN",
		Value:    values,
	})
	return qb
}

func (qb *QueryBuilder) WhereNotIn(field string, values interface{}) *QueryBuilder {
	qb.conditions = append(qb.conditions, Condition{
		Field:    field,
		Operator: "NOT IN",
		Value:    values,
	})
	return qb
}

func (qb *QueryBuilder) WhereLike(field string, value string) *QueryBuilder {
	qb.conditions = append(qb.conditions, Condition{
		Field:    field,
		Operator: "LIKE",
		Value:    value,
	})
	return qb
}

// Search functionality
func (qb *QueryBuilder) Search(fields []string, text string) *QueryBuilder {
	qb.searchFields = fields
	qb.searchText = text
	return qb
}

// ORDER BY
func (qb *QueryBuilder) OrderBy(field, direction string) *QueryBuilder {
	qb.orderBy = append(qb.orderBy, fmt.Sprintf("%s %s", field, strings.ToUpper(direction)))
	return qb
}

func (qb *QueryBuilder) OrderByAsc(field string) *QueryBuilder {
	return qb.OrderBy(field, "ASC")
}

func (qb *QueryBuilder) OrderByDesc(field string) *QueryBuilder {
	return qb.OrderBy(field, "DESC")
}

// GROUP BY and HAVING
func (qb *QueryBuilder) GroupBy(fields ...string) *QueryBuilder {
	qb.groupBy = append(qb.groupBy, fields...)
	return qb
}

func (qb *QueryBuilder) Having(condition string) *QueryBuilder {
	qb.having = append(qb.having, condition)
	return qb
}

// LIMIT and OFFSET
func (qb *QueryBuilder) Limit(limit int) *QueryBuilder {
	qb.limit = limit
	return qb
}

func (qb *QueryBuilder) Offset(offset int) *QueryBuilder {
	qb.offset = offset
	return qb
}

// INSERT operations
func (qb *QueryBuilder) Insert(model interface{}) *QueryBuilder {
	qb.queryType = "INSERT"
	qb.insertModel = model
	return qb
}

func (qb *QueryBuilder) Into(table string) *QueryBuilder {
	qb.table = table
	return qb
}

func (qb *QueryBuilder) Values(values map[string]interface{}) *QueryBuilder {
	qb.values = values
	return qb
}

// UPDATE operations
func (qb *QueryBuilder) Update(table string) *QueryBuilder {
	qb.queryType = "UPDATE"
	qb.table = table
	return qb
}

func (qb *QueryBuilder) Set(field string, value interface{}) *QueryBuilder {
	if qb.values == nil {
		qb.values = make(map[string]interface{})
	}
	qb.values[field] = value
	return qb
}

func (qb *QueryBuilder) SetModel(model interface{}) *QueryBuilder {
	qb.updateModel = model
	return qb
}

// DELETE operations
func (qb *QueryBuilder) Delete() *QueryBuilder {
	qb.queryType = "DELETE"
	return qb
}

func (qb *QueryBuilder) DeleteFrom(table string) *QueryBuilder {
	qb.queryType = "DELETE"
	qb.table = table
	return qb
}

// Build the final SQL query using existing centralized functions
func (qb *QueryBuilder) Build() (string, []interface{}, error) {
	switch qb.queryType {
	case "SELECT":
		return qb.buildSelect()
	case "INSERT":
		return qb.buildInsert()
	case "UPDATE":
		return qb.buildUpdate()
	case "DELETE":
		return qb.buildDelete()
	default:
		return "", nil, fmt.Errorf("unsupported query type: %s", qb.queryType)
	}
}

func (qb *QueryBuilder) buildSelect() (string, []interface{}, error) {
	if qb.table == "" {
		return "", nil, fmt.Errorf("table name is required for SELECT")
	}

	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(qb.fields, ", "), qb.table)

	// Add JOINs
	for _, join := range qb.joins {
		query += " " + join
	}

	// Add WHERE conditions using centralized function
	var args []interface{}
	if len(qb.conditions) > 0 || len(qb.searchFields) > 0 {
		whereClause, whereArgs := buildConditionsWithSearch(qb.conditions, qb.searchFields, qb.searchText, args)
		if whereClause != "" {
			query += " WHERE " + whereClause
			args = whereArgs
		}
	}

	// Add GROUP BY
	if len(qb.groupBy) > 0 {
		query += " GROUP BY " + strings.Join(qb.groupBy, ", ")
	}

	// Add HAVING
	if len(qb.having) > 0 {
		query += " HAVING " + strings.Join(qb.having, " AND ")
	}

	// Add ORDER BY
	if len(qb.orderBy) > 0 {
		query += " ORDER BY " + strings.Join(qb.orderBy, ", ")
	}

	// Add LIMIT
	if qb.limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", qb.limit)
	}

	// Add OFFSET
	if qb.offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", qb.offset)
	}

	return query, args, nil
}

func (qb *QueryBuilder) buildInsert() (string, []interface{}, error) {
	if qb.table == "" {
		return "", nil, fmt.Errorf("table name is required for INSERT")
	}

	if qb.insertModel != nil {
		// Use existing buildInsertStmt function
		insertParams := &DatabaseInsert{Table: qb.table}
		parseTags(qb.insertModel, &insertParams.Fields)
		return buildInsertStmt(insertParams, qb.insertModel)
	}

	if len(qb.values) == 0 {
		return "", nil, fmt.Errorf("values are required for INSERT")
	}

	var fields []string
	var placeholders []string
	var args []interface{}

	for field, value := range qb.values {
		fields = append(fields, field)
		placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)+1))
		args = append(args, value)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		qb.table,
		strings.Join(fields, ", "),
		strings.Join(placeholders, ", "))

	return query, args, nil
}

func (qb *QueryBuilder) buildUpdate() (string, []interface{}, error) {
	if qb.table == "" {
		return "", nil, fmt.Errorf("table name is required for UPDATE")
	}

	if qb.updateModel != nil {
		// Use existing buildUpdateStmt function
		updateParams := &DatabaseUpdate{
			Table:      qb.table,
			Conditions: qb.conditions,
		}
		return buildUpdateStmt(updateParams, qb.updateModel)
	}

	if len(qb.values) == 0 {
		return "", nil, fmt.Errorf("values are required for UPDATE")
	}

	query := fmt.Sprintf("UPDATE %s SET ", qb.table)
	var args []interface{}

	var setParts []string
	for field, value := range qb.values {
		setParts = append(setParts, fmt.Sprintf("%s = $%d", field, len(args)+1))
		args = append(args, value)
	}

	query += strings.Join(setParts, ", ")

	// Add WHERE conditions using centralized function
	if len(qb.conditions) > 0 {
		whereClause, whereArgs := buildConditions(qb.conditions, args)
		if whereClause != "" {
			query += " WHERE " + whereClause
			args = whereArgs
		}
	}

	return query, args, nil
}

func (qb *QueryBuilder) buildDelete() (string, []interface{}, error) {
	if qb.table == "" {
		return "", nil, fmt.Errorf("table name is required for DELETE")
	}

	query := fmt.Sprintf("DELETE FROM %s", qb.table)

	// Add WHERE conditions using centralized function
	var args []interface{}
	if len(qb.conditions) > 0 {
		whereClause, whereArgs := buildConditions(qb.conditions, args)
		if whereClause != "" {
			query += " WHERE " + whereClause
			args = whereArgs
		}
	}

	return query, args, nil
}

// getPrimaryKeyField returns the database column name of the primary key field from a struct
func getPrimaryKeyField(model interface{}) string {
	val := reflect.ValueOf(model)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	t := val.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		// Check if this field has the primary key tag
		if gpoField := parseGPOTag(field); gpoField != nil && gpoField.IsPrimaryKey {
			return gpoField.ColumnName
		}
	}
	// Fallback to default if no primary key tag is found
	return DefaultIDField
}

// isPrimaryKeyField checks if a field is marked as primary key
func isPrimaryKeyField(field reflect.StructField) bool {
	gpoField := parseGPOTag(field)
	return gpoField != nil && gpoField.IsPrimaryKey
}

// scanRowToModel creates scan arguments for a single row based on field mapping
func scanRowToModel(columns []string, fieldMap FieldMap, modelVal reflect.Value) []interface{} {
	scanArgs := make([]interface{}, len(columns))
	for i, column := range columns {
		if field, ok := fieldMap[column]; ok {
			fieldVal := modelVal.FieldByName(field)
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
	return scanArgs
}

// prepareStatement prepares a SQL statement with optional transaction support
func prepareStatement(ctx context.Context, tx *sql.Tx, db *sql.DB, query string) (*sql.Stmt, error) {
	if tx != nil {
		return tx.PrepareContext(ctx, query)
	}
	return db.PrepareContext(ctx, query)
}

// createPrimaryKeyCondition creates a condition for primary key lookup
func createPrimaryKeyCondition(model interface{}, idValue interface{}) []Condition {
	pkField := getPrimaryKeyField(model)
	return []Condition{
		{
			Field:    pkField,
			Operator: "=",
			Value:    idValue,
		},
	}
}
