package db

import (
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
		if tag, ok := typeField.Tag.Lookup("db_column"); ok {
			*fields = append(*fields, tag)
			fieldMap[tag] = typeField.Name
		}
	}
	return fieldMap
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
					fmt.Println("error converting column length to int, length will be 0.")
				}
			}
			columnType := convertGoTypeToPostgresType(field.Type.Name(), length)
			_, unique := field.Tag.Lookup("db_unique")
			_, nullable := field.Tag.Lookup("db_nullable")
			columns = append(columns, Column{Name: columnName, Type: columnType, PrimaryKey: columnName == "id", Unique: unique, Null: nullable, Length: length})
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
			args = append(args, "%"+params.SearchText+"%")
		}
	}
	ob := params.OrderBy
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
	} else {
		query += " LIMIT 10"
	}
	if params.Offset > 0 {
		query += fmt.Sprintf(" OFFSET $%d", len(args)+1)
		args = append(args, params.Offset)
	}
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
