package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

type Condition struct {
	Field    string `json:"field"`
	Operator string `json:"operator"`
	Value    Value  `json:"value"`
}

type DatabaseQuery struct {
	Table           string      `json:"table"`
	Fields          Fields      `json:"fields"`
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
)

type Fields []string

func (f Fields) String() []string {
	var fields []string
	for _, field := range f {
		fields = append(fields, field)
	}
	return fields
}

type Value struct {
	vType FieldType
	value interface{}
}

func NewValue(value interface{}) (*Value, error) {
	v := &Value{}
	err := v.Set(value)
	if err != nil {
		return nil, err
	}
	return v, nil
}

// don't use this unless you know what you're doing
func MustNewValue(value interface{}) Value {
	v, err := NewValue(value)
	if err != nil {
		panic(err)
	}
	return *v
}

// method for Value when converted from JSON
func (v *Value) UnmarshalJSON(b []byte) error {
	var value interface{}
	err := json.Unmarshal(b, &value)
	if err != nil {
		return err
	}
	return v.Set(value)
}
func (v *Value) MarshalJSON() ([]byte, error) {
	// Convert v.value to JSON
	b, err := json.Marshal(v.value)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (v *Value) Set(value interface{}) error {
	switch value.(type) {
	case int:
		v.vType = IntType
		v.value = value.(int)
	case string:
		v.vType = StringType
		v.value = value.(string)
	default:
		return errors.New(fmt.Sprintf("invalid type: %T", value))
	}
	return nil
}
func (v Value) Get() interface{} {
	return v.value
}
func (v Value) String() string {
	if v.vType == IntType {
		return fmt.Sprintf("%d", v.value.(int))
	}
	return v.value.(string)
}

func (v Value) GetTypeName() string {
	if v.vType == IntType {
		return "int"
	}
	return "string"
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
	var query string
	query = fmt.Sprintf("SELECT %s FROM %s", strings.Join(params.Fields.String(), ","), params.Table)
	if len(params.Condition) > 0 {
		query += " WHERE "
		for i, condition := range params.Condition {
			if i > 0 {
				query += " AND "
			}
			if condition.Operator == "LIKE" || condition.Operator == "NOT LIKE" {
				query += fmt.Sprintf("%s %s '%%%s%%'", condition.Field, condition.Operator, condition.Value.String())
			} else {
				query += fmt.Sprintf("%s %s %s", condition.Field, condition.Operator, condition.Value.String())
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
	var query string
	query = fmt.Sprintf("SELECT %s FROM %s", strings.Join(params.Fields.String(), ","), params.Table)
	if len(params.Condition) > 0 {
		query += " WHERE "
		for i, condition := range params.Condition {
			if i > 0 {
				query += " AND "
			}
			if condition.Operator == "LIKE" || condition.Operator == "NOT LIKE" {
				query += fmt.Sprintf("%s %s '%%%s%%'", condition.Field, condition.Operator, condition.Value.String())
			} else {
				query += fmt.Sprintf("%s %s %s", condition.Field, condition.Operator, condition.Value.String())
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
	var query string
	query = fmt.Sprintf("SELECT %s FROM %s", strings.Join(params.Fields.String(), ","), params.Table)
	if len(params.Condition) > 0 {
		query += " WHERE "
		for i, condition := range params.Condition {
			if i > 0 {
				query += " AND "
			}
			if condition.Operator == "LIKE" || condition.Operator == "NOT LIKE" {
				query += fmt.Sprintf("%s %s '%%%s%%'", condition.Field, condition.Operator, condition.Value.String())
			} else {
				query += fmt.Sprintf("%s %s %s", condition.Field, condition.Operator, condition.Value.String())
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
	// Connect to the SQLite database
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

func (s SqlHandler) Query(r *http.Request, queryProps DatabaseQuery) (rows *sql.Rows, err error) {
	// Connect to the SQLite database
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
				return nil, errors.New(fmt.Sprintf("invalid field '%s' used for orderBy", orderBy))
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
