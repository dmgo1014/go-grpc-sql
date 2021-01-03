package protocol

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"time"

	"google.golang.org/protobuf/proto"
)

//go:generate go install google.golang.org/protobuf/cmd/protoc-gen-go google.golang.org/grpc/cmd/protoc-gen-go-grpc
//go:generate protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=.  --go-grpc_opt=paths=source_relative sql.proto

// NewRequestOpen creates a new Request of type RequestOpen.
func NewRequestOpen(name string) *Request {
	return newRequest(&RequestOpen{Name: name})
}

// NewRequestPrepare creates a new Request of type RequestPrepare.
func NewRequestPrepare(query string) *Request {
	return newRequest(&RequestPrepare{Query: query})
}

// NewRequestExec creates a new Request of type RequestExec.
func NewRequestExec(id int64, args []*Value) *Request {
	return newRequest(&RequestExec{Id: id, Args: args})
}

// NewRequestQuery creates a new Request of type RequestQuery.
func NewRequestQuery(id int64, args []*Value) *Request {
	return newRequest(&RequestQuery{Id: id, Args: args})
}

// NewRequestNext creates a new Request of type RequestNext.
func NewRequestNext(id int64, n int64) *Request {
	return newRequest(&RequestNext{Id: id, Len: n})
}

// NewRequestColumnTypeScanType creates a new Request of type ColumnTypeScanType.
func NewRequestColumnTypeScanType(id int64, column int64) *Request {
	return newRequest(&RequestColumnTypeScanType{Id: id, Column: column})
}

// NewRequestColumnTypeDatabaseTypeName creates a new Request of type ColumnTypeDatabaseTypeName.
func NewRequestColumnTypeDatabaseTypeName(id int64, column int64) *Request {
	return newRequest(&RequestColumnTypeDatabaseTypeName{Id: id, Column: column})
}

// NewRequestRowsClose creates a new Request of type RequestRowsClose.
func NewRequestRowsClose(id int64) *Request {
	return newRequest(&RequestRowsClose{Id: id})
}

// NewRequestStmtClose creates a new Request of type RequestStmtClose.
func NewRequestStmtClose(id int64) *Request {
	return newRequest(&RequestStmtClose{Id: id})
}

// NewRequestBegin creates a new Request of type RequestBegin.
func NewRequestBegin() *Request {
	return newRequest(&RequestBegin{})
}

// NewRequestCommit creates a new Request of type RequestCommit.
func NewRequestCommit(id int64) *Request {
	return newRequest(&RequestCommit{Id: id})
}

// NewRequestRollback creates a new Request of type RequestRollback.
func NewRequestRollback(id int64) *Request {
	return newRequest(&RequestRollback{Id: id})
}

// NewRequestClose creates a new Request of type RequestClose.
func NewRequestClose() *Request {
	return newRequest(&RequestClose{})
}

// NewRequestConnExec creates a new Request of type RequestConnExec.
func NewRequestConnExec(query string, args []*Value) *Request {
	return newRequest(&RequestConnExec{Query: query, Args: args})
}

// Create a new Request with the given payload.
func newRequest(message proto.Message) *Request {
	var code RequestCode
	switch message.(type) {
	case *RequestOpen:
		code = RequestCode_OPEN
	case *RequestPrepare:
		code = RequestCode_PREPARE
	case *RequestExec:
		code = RequestCode_EXEC
	case *RequestQuery:
		code = RequestCode_QUERY
	case *RequestNext:
		code = RequestCode_NEXT
	case *RequestColumnTypeScanType:
		code = RequestCode_COLUMN_TYPE_SCAN_TYPE
	case *RequestColumnTypeDatabaseTypeName:
		code = RequestCode_COLUMN_TYPE_DATABASE_TYPE_NAME
	case *RequestRowsClose:
		code = RequestCode_ROWS_CLOSE
	case *RequestStmtClose:
		code = RequestCode_STMT_CLOSE
	case *RequestBegin:
		code = RequestCode_BEGIN
	case *RequestCommit:
		code = RequestCode_COMMIT
	case *RequestRollback:
		code = RequestCode_ROLLBACK
	case *RequestClose:
		code = RequestCode_CLOSE
	case *RequestConnExec:
		code = RequestCode_CONN_EXEC
	default:
		panic(fmt.Errorf("invalid message type: %s", reflect.TypeOf(message).Kind()))
	}

	data, err := proto.Marshal(message)
	if err != nil {
		panic(fmt.Errorf("cannot marshal %s request", code))
	}

	request := &Request{
		Code: code,
		Data: data,
	}

	return request
}

// NewResponseOpen creates a new Response of type ResponseOpen.
func NewResponseOpen() *Response {
	return newResponse(&ResponseOpen{})
}

// NewResponsePrepare creates a new Response of type ResponsePrepare.
func NewResponsePrepare(id int64, numInput int) *Response {
	return newResponse(&ResponsePrepare{Id: id, NumInput: int64(numInput)})
}

// NewResponseExec creates a new Response of type ResponseExec.
func NewResponseExec(lastInsertID, rowsAffected int64) *Response {
	return newResponse(&ResponseExec{
		LastInsertId: lastInsertID,
		RowsAffected: rowsAffected,
	})
}

// NewResponseQuery creates a new Response of type ResponseQuery.
func NewResponseQuery(id int64, columns []string) *Response {
	return newResponse(&ResponseQuery{
		Id:      id,
		Columns: columns,
	})
}

// NewResponseNext creates a new Response of type ResponseNext.
func NewResponseNext(eof bool, values []*Value) *Response {
	return newResponse(&ResponseNext{
		Eof:    eof,
		Values: values,
	})
}

// NewResponseColumnTypeScanType creates a new Response of type ResponseColumnTypeScanType.
func NewResponseColumnTypeScanType(code ValueCode) *Response {
	return newResponse(&ResponseColumnTypeScanType{
		Code: code,
	})
}

// NewResponseColumnTypeDatabaseTypeName creates a new Response of type ResponseColumnTypeDatabaseTypeName.
func NewResponseColumnTypeDatabaseTypeName(name string) *Response {
	return newResponse(&ResponseColumnTypeDatabaseTypeName{
		Name: name,
	})
}

// NewResponseRowsClose creates a new Response of type ResponseRowsClose.
func NewResponseRowsClose() *Response {
	return newResponse(&ResponseRowsClose{})
}

// NewResponseStmtClose creates a new Response of type ResponseStmtClose.
func NewResponseStmtClose() *Response {
	return newResponse(&ResponseStmtClose{})
}

// NewResponseBegin creates a new Response of type ResponseBegin.
func NewResponseBegin(id int64) *Response {
	return newResponse(&ResponseBegin{Id: id})
}

// NewResponseCommit creates a new Response of type ResponseCommit.
func NewResponseCommit() *Response {
	return newResponse(&ResponseCommit{})
}

// NewResponseRollback creates a new Response of type ResponseRollback.
func NewResponseRollback() *Response {
	return newResponse(&ResponseRollback{})
}

// NewResponseClose creates a new Response of type ResponseClose.
func NewResponseClose() *Response {
	return newResponse(&ResponseClose{})
}

// NewResponseSQLError creates a new Response of type ResponseSQLError.
func NewResponseSQLError(code, extendedCode int32, err string) *Response {
	return newResponse(&ResponseSQLError{
		Code:         code,
		ExtendedCode: extendedCode,
		Err:          err,
	})
}

// Prepare returns a ResponsePrepare payload.
func (r *Response) Prepare() *ResponsePrepare {
	message := &ResponsePrepare{}
	r.unmarshal(message)
	return message
}

// Exec returns a ResponseExec payload.
func (r *Response) Exec() *ResponseExec {
	message := &ResponseExec{}
	r.unmarshal(message)
	return message
}

// Query returns a ResponseQuery payload.
func (r *Response) Query() *ResponseQuery {
	message := &ResponseQuery{}
	r.unmarshal(message)
	return message
}

// Next returns a ResponseNext payload.
func (r *Response) Next() *ResponseNext {
	message := &ResponseNext{}
	r.unmarshal(message)
	return message
}

// ColumnTypeScanType returns a ResponseColumnTypeScanType payload.
func (r *Response) ColumnTypeScanType() *ResponseColumnTypeScanType {
	message := &ResponseColumnTypeScanType{}
	r.unmarshal(message)
	return message
}

// ColumnTypeDatabaseTypeName returns a ResponseColumnTypeDatabaseTypeName payload.
func (r *Response) ColumnTypeDatabaseTypeName() *ResponseColumnTypeDatabaseTypeName {
	message := &ResponseColumnTypeDatabaseTypeName{}
	r.unmarshal(message)
	return message
}

// Begin returns a ResponseBegin payload.
func (r *Response) Begin() *ResponseBegin {
	message := &ResponseBegin{}
	r.unmarshal(message)
	return message
}

// SQLError returns a ResponseSQLError payload.
func (r *Response) SQLError() *ResponseSQLError {
	message := &ResponseSQLError{}
	r.unmarshal(message)
	return message
}

func (r *Response) unmarshal(message proto.Message) {
	if err := proto.Unmarshal(r.Data, message); err != nil {
		panic(fmt.Errorf("failed to unmarshal response: %v", err))
	}
}

// Create a new Response with the given payload.
func newResponse(message proto.Message) *Response {
	var code RequestCode
	switch message.(type) {
	case *ResponseOpen:
		code = RequestCode_OPEN
	case *ResponsePrepare:
		code = RequestCode_PREPARE
	case *ResponseExec:
		code = RequestCode_EXEC
	case *ResponseQuery:
		code = RequestCode_QUERY
	case *ResponseNext:
		code = RequestCode_NEXT
	case *ResponseColumnTypeScanType:
		code = RequestCode_COLUMN_TYPE_SCAN_TYPE
	case *ResponseColumnTypeDatabaseTypeName:
		code = RequestCode_COLUMN_TYPE_DATABASE_TYPE_NAME
	case *ResponseRowsClose:
		code = RequestCode_ROWS_CLOSE
	case *ResponseBegin:
		code = RequestCode_BEGIN
	case *ResponseCommit:
		code = RequestCode_COMMIT
	case *ResponseRollback:
		code = RequestCode_ROLLBACK
	case *ResponseStmtClose:
		code = RequestCode_STMT_CLOSE
	case *ResponseClose:
		code = RequestCode_CLOSE
	case *ResponseSQLError:
		code = RequestCode_SQL_ERROR
	default:
		panic(fmt.Errorf("invalid message type"))
	}

	data, err := proto.Marshal(message)
	if err != nil {
		panic(fmt.Sprintf("cannot marshal %s response", code))
	}

	response := &Response{
		Code: code,
		Data: data,
	}

	return response
}

// FromDriverValues converts a slice of Go driver.Value objects of supported
// types to a slice of protobuf Value objects.
func FromDriverValues(objects []driver.Value) ([]*Value, error) {
	values := make([]*Value, len(objects))
	for i, object := range objects {
		value, err := toValue(object)
		if err != nil {
			return nil, fmt.Errorf("cannot marshal object %d (%v): %s", i, object, err)
		}
		values[i] = &value
	}
	return values, nil
}

// ToValueSlice converts a slice of Go objects of supported types to a slice of
// protobuf Value objects.
func ToValueSlice(objects []interface{}) ([]Value, error) {
	values := make([]Value, len(objects))
	for i, object := range objects {
		value, err := toValue(object)
		if err != nil {
			return nil, fmt.Errorf("cannot marshal object %d (%v): %s", i, object, err)
		}
		values[i] = value
	}
	return values, nil
}

// ToValueCode converts a Go type object into its serialized code number.
func ToValueCode(t reflect.Type) ValueCode {
	var code ValueCode
	switch t {
	case reflect.TypeOf(int64(0)):
		code = ValueCode_INT64
	case reflect.TypeOf(float64(0)):
		code = ValueCode_FLOAT64
	case reflect.TypeOf(false):
		code = ValueCode_BOOL
	case reflect.TypeOf(byte(0)):
		code = ValueCode_BYTES
	case reflect.TypeOf(""):
		code = ValueCode_STRING
	case reflect.TypeOf(time.Time{}):
		code = ValueCode_TIME
	case reflect.TypeOf(nil):
		code = ValueCode_NULL
	default:
		code = ValueCode_BYTES
	}
	return code
}

// FromValueCode converts a serialized value type code into a Go type object.
func FromValueCode(code ValueCode) reflect.Type {
	var t reflect.Type
	switch code {
	case ValueCode_INT64:
		t = reflect.TypeOf(int64(0))
	case ValueCode_FLOAT64:
		t = reflect.TypeOf(float64(0))
	case ValueCode_BOOL:
		t = reflect.TypeOf(false)
	case ValueCode_BYTES:
		t = reflect.TypeOf(byte(0))
	case ValueCode_STRING:
		t = reflect.TypeOf("")
	case ValueCode_TIME:
		t = reflect.TypeOf(time.Time{})
	case ValueCode_NULL:
		t = reflect.TypeOf(nil)
	default:
		t = reflect.TypeOf(byte(0))
	}
	return t
}

// Convert a Go object of a supported type to a protobuf Value object.
func toValue(value interface{}) (Value, error) {
	switch v := value.(type) {
	case int64:
		return Value{Code: ValueCode_INT64, Int64: v}, nil
	case float64:
		return Value{Code: ValueCode_FLOAT64, Float64: v}, nil
	case bool:
		return Value{Code: ValueCode_BOOL, Bool: v}, nil
	case []byte:
		return Value{Code: ValueCode_BYTES, Bytes: v}, nil
	case string:
		return Value{Code: ValueCode_STRING, Text: v}, nil
	case time.Time:
		return Value{Code: ValueCode_TIME, Time: v.Unix()}, nil
	default:
		if value != nil {
			return Value{Code: ValueCode_NULL}, fmt.Errorf("invalid type %s", reflect.TypeOf(value).Kind())
		}
		return Value{Code: ValueCode_NULL}, nil
	}
}

// ToDriverValues converts a slice of protobuf Value objects to a slice of Go
// driver.Value objects.
func ToDriverValues(values []*Value) ([]driver.Value, error) {
	args, err := FromValueSlice(values)
	if err != nil {
		return nil, err
	}
	a := make([]driver.Value, len(args))
	for i, arg := range args {
		a[i] = arg
	}
	return a, nil
}

// FromValueSlice converts a slice of protobuf Value objects to a slice of Go
// interface{} objects.
func FromValueSlice(values []*Value) ([]interface{}, error) {
	objects := make([]interface{}, len(values))
	for i, value := range values {
		objects[i] = value.Interface()
	}
	return objects, nil
}

// Interface implements valueMessage.
func (v *Value) Interface() interface{} {
	if v == nil {
		return nil
	}
	switch v.Code {
	case ValueCode_NULL:
		return nil
	case ValueCode_INT64:
		return v.Int64
	case ValueCode_FLOAT64:
		return v.Float64
	case ValueCode_BOOL:
		return v.Bool
	case ValueCode_BYTES:
		return v.Bytes
	case ValueCode_STRING:
		return v.Text
	case ValueCode_TIME:
		return time.Unix(v.Time, 0)
	default:
		panic(fmt.Errorf("unknown ValueCode %d", v.Code))
	}
}

func (sqle *ResponseSQLError) Error() string {
	if sqle == nil {
		return ""
	}
	return fmt.Sprintf("%d[%d]: %s", sqle.Code, sqle.ExtendedCode, sqle.Err)
}
