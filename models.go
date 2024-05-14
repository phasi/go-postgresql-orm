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

type SqlHandler struct {
	DriverName     string
	DatasourceName string
}

func NewSqlHandler(driverName string, datasourceName string) *SqlHandler {
	return &SqlHandler{
		DriverName:     driverName,
		DatasourceName: datasourceName,
	}
}

func (s *SqlHandler) buildQuery(params *DatabaseQuery) string {
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

func (s *SqlHandler) buildPaginatedQuery(params *DatabaseQuery, limit int, offset int, orderBy string) string {
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

func (s *SqlHandler) buildSearchQuery(params *DatabaseQuery, searchText string) string {
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
	if len(params.SearchFields) > 0 {
		query += " WHERE "
		for i, field := range params.SearchFields {
			if i > 0 {
				query += " OR "
			}
			query += fmt.Sprintf("%s LIKE '%%%s%%'", field, searchText)
		}
	}
	return query
}

func (s *SqlHandler) buildInsertQuery(params *DatabaseInsert, data *[]map[string]interface{}) (string, []interface{}, error) {
	var query string
	var args []interface{}
	query = fmt.Sprintf("INSERT INTO %s (%s) VALUES ", params.Table, strings.Join(params.Fields.String(), ","))
	for i, row := range *data {
		if i > 0 {
			query += ","
		}
		query += "("
		for j, field := range params.Fields {
			if j > 0 {
				query += ","
			}
			// Use $1, $2, etc. as placeholders instead of ?
			query += fmt.Sprintf("$%d", len(args)+1)
			args = append(args, row[field])
		}
		query += ")"
	}
	return query, args, nil
}

func (s SqlHandler) Insert(ctx context.Context, insertProps DatabaseInsert, data *[]map[string]interface{}) (err error) {
	q, args, err := s.buildInsertQuery(&insertProps, data)
	if err != nil {
		return
	}
	db, err := sql.Open(s.DriverName, s.DatasourceName)
	if err != nil {
		return
	}
	defer db.Close()

	// Prepare the query
	stmt, err := db.PrepareContext(ctx, q)
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

func (s SqlHandler) First(tableName string, model interface{}, condition []Condition) error {
	var queryProps DatabaseQuery
	queryProps.Table = tableName
	queryProps.Model = model
	queryProps.Condition = condition
	queryProps.Limit = 1
	fieldMap := parseTags(model, &queryProps.Fields)
	rows, err := s.Query(nil, queryProps)
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

func (s SqlHandler) All(tableName string, model interface{}, condition []Condition) ([]interface{}, error) {
	var queryProps DatabaseQuery
	queryProps.Table = tableName
	queryProps.Model = model
	queryProps.Condition = condition
	fieldMap := parseTags(model, &queryProps.Fields)
	rows, err := s.Query(nil, queryProps)
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

func (s SqlHandler) RichQuery(r *http.Request, queryProps *DatabaseQuery) ([]interface{}, error) {
	var fieldMap FieldMap
	if queryProps.Model != nil {
		fieldMap = parseTags(queryProps.Model, &queryProps.Fields)
	}
	rows, err := s.Query(r, *queryProps)
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

func (s SqlHandler) Query(r *http.Request, queryProps DatabaseQuery) (rows *sql.Rows, err error) {
	var q string
	if queryProps.AllowPagination {
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
		q = s.buildPaginatedQuery(&queryProps, limit, offset, orderBy)
	} else if queryProps.AllowSearch {
		searchText := r.URL.Query().Get("search")
		q = s.buildSearchQuery(&queryProps, searchText)
	} else {
		q = s.buildQuery(&queryProps)
	}
	db, err := sql.Open(s.DriverName, s.DatasourceName)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// Perform a query
	rows, err = db.Query(q)
	if err != nil {
		return nil, err
	}
	return rows, nil
}
