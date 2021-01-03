package grpcsql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/godror/go-grpc-sql/internal/protocol"
	"google.golang.org/protobuf/proto"
)

// Gateway mapping gRPC requests to SQL queries.
type Gateway struct {
	driver driver.DriverContext // Underlying SQL driver.
	conn   *gatewayConn
	protocol.UnimplementedSQLServer
}

// NewGateway creates a new gRPC gateway executing requests against the given
// SQL driver.
func NewGateway(drv driver.DriverContext) *Gateway {
	if drv == nil {
		panic("nil Driver")
	}
	return &Gateway{
		driver: drv,
		conn:   &gatewayConn{driver: drv},
	}
}

func (s *Gateway) Close() error {
	driver, conn := s.driver, s.conn
	s.driver, s.conn = nil, nil
	if conn != nil {
		conn.Close()
	}
	if driver != nil {
		if c, ok := driver.(interface{ Close() error }); ok {
			c.Close()
		}
	}
	return nil
}

// Conn creates a new database connection using the underlying driver, and
// start accepting requests for it.
func (s *Gateway) Conn(stream protocol.SQL_ConnServer) error {
	defer s.conn.rollback()
	for {
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to receive request: %w", err)
		}

		response, err := s.conn.handle(request)
		if err != nil {
			// TODO: add support for more driver-specific errors.
			return fmt.Errorf("failed to handle %s request: %w", request.Code, err)
		}

		if err := stream.Send(response); err != nil {
			return fmt.Errorf("failed to send response: %w", err)
		}
	}
}

type driverStmt interface {
	driver.StmtExecContext
	driver.StmtQueryContext
	driver.Stmt
}

type driverConn interface {
	driver.ConnBeginTx
	driver.ConnPrepareContext
	driver.Conn
}

// Track a single driver connection
type gatewayConn struct {
	driver         driver.DriverContext
	driverConn     driverConn
	stmts          map[int64]driverStmt
	txs            map[int64]driver.Tx
	rows           map[int64]driver.Rows
	serial         int64
	mu             sync.Mutex
	refcount       int
	txCh           chan struct{}
	noLastInsertID bool
}

// Handle a single gRPC request for this connection.
func (c *gatewayConn) handle(request *protocol.Request) (*protocol.Response, error) {
	var message proto.Message

	switch request.Code {
	case protocol.RequestCode_OPEN:
		message = &protocol.RequestOpen{}
	case protocol.RequestCode_PREPARE:
		message = &protocol.RequestPrepare{}
	case protocol.RequestCode_EXEC:
		message = &protocol.RequestExec{}
	case protocol.RequestCode_QUERY:
		message = &protocol.RequestQuery{}
	case protocol.RequestCode_NEXT:
		message = &protocol.RequestNext{}
	case protocol.RequestCode_COLUMN_TYPE_SCAN_TYPE:
		message = &protocol.RequestColumnTypeScanType{}
	case protocol.RequestCode_COLUMN_TYPE_DATABASE_TYPE_NAME:
		message = &protocol.RequestColumnTypeDatabaseTypeName{}
	case protocol.RequestCode_ROWS_CLOSE:
		message = &protocol.RequestRowsClose{}
	case protocol.RequestCode_STMT_CLOSE:
		message = &protocol.RequestStmtClose{}
	case protocol.RequestCode_BEGIN:
		message = &protocol.RequestBegin{}
	case protocol.RequestCode_COMMIT:
		message = &protocol.RequestCommit{}
	case protocol.RequestCode_ROLLBACK:
		message = &protocol.RequestRollback{}
	case protocol.RequestCode_CLOSE:
		message = &protocol.RequestClose{}
	case protocol.RequestCode_CONN_EXEC:
		message = &protocol.RequestConnExec{}
	default:
		return nil, fmt.Errorf("invalid request code %d", request.Code)
	}

	if err := proto.Unmarshal(request.Data, message); err != nil {
		return nil, fmt.Errorf("request parse error: %w", err)
	}

	// The very first request must be an OPEN one.
	if c.driverConn == nil && request.Code != protocol.RequestCode_OPEN {
		return nil, fmt.Errorf("expected OPEN request, got %s", request.Code)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	switch r := message.(type) {
	case *protocol.RequestOpen:
		return c.handleOpen(ctx, r)
	case *protocol.RequestPrepare:
		return c.handlePrepare(ctx, r)
	case *protocol.RequestExec:
		return c.handleExec(ctx, r)
	case *protocol.RequestQuery:
		return c.handleQuery(ctx, r)
	case *protocol.RequestNext:
		return c.handleNext(ctx, r)
	case *protocol.RequestColumnTypeScanType:
		return c.handleColumnTypeScanType(ctx, r)
	case *protocol.RequestColumnTypeDatabaseTypeName:
		return c.handleColumnTypeDatabaseTypeName(ctx, r)
	case *protocol.RequestRowsClose:
		return c.handleRowsClose(ctx, r)
	case *protocol.RequestStmtClose:
		return c.handleStmtClose(ctx, r)
	case *protocol.RequestBegin:
		return c.handleBegin(ctx, r)
	case *protocol.RequestCommit:
		return c.handleCommit(ctx, r)
	case *protocol.RequestRollback:
		return c.handleRollback(ctx, r)
	case *protocol.RequestClose:
		return c.handleClose(ctx, r)
	case *protocol.RequestConnExec:
		return c.handleConnExec(ctx, r)
	default:
		panic("unhandled request payload type")
	}
}

// Handle a request of type OPEN.
func (c *gatewayConn) handleOpen(ctx context.Context, request *protocol.RequestOpen) (*protocol.Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.refcount > 0 {
		c.refcount++
		return protocol.NewResponseOpen(), nil
	}
	connector, err := c.driver.OpenConnector(request.Name)
	if err != nil {
		return nil, fmt.Errorf("could not open driver connection: %w", err)
	}

	cx, err := connector.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not connect to %q: %w", request.Name, err)
	}
	c.driverConn = cx.(driverConn)
	c.stmts = make(map[int64]driverStmt)
	c.txs = make(map[int64]driver.Tx)
	c.rows = make(map[int64]driver.Rows)

	c.refcount++
	response := protocol.NewResponseOpen()
	return response, nil
}

func (c *gatewayConn) abort() {
	for id, rows := range c.rows {
		rows.Close()
		delete(c.rows, id)
	}
	for id, stmt := range c.stmts {
		stmt.Close()
		delete(c.stmts, id)
	}
}

// Handle a request of type PREPARE.
func (c *gatewayConn) handlePrepare(ctx context.Context, request *protocol.RequestPrepare) (*protocol.Response, error) {
	if len(c.txs) != 1 {
		return nil, fmt.Errorf("not in a transaction")
	}
	stmt, err := c.driverConn.PrepareContext(ctx, request.Query)
	if err != nil {
		return nil, err
	}
	c.serial++
	c.stmts[c.serial] = stmt.(driverStmt)
	return protocol.NewResponsePrepare(c.serial, stmt.NumInput()), nil
}

// Handle a request of type EXEC.
func (c *gatewayConn) handleExec(ctx context.Context, request *protocol.RequestExec) (*protocol.Response, error) {
	if len(c.txs) != 1 {
		return nil, fmt.Errorf("not in a transaction")
	}
	driverStmt, ok := c.stmts[request.Id]
	if !ok {
		return nil, fmt.Errorf("no prepared statement with ID %d", request.Id)
	}

	args, err := protocol.ToDriverValues(request.Args)
	if err != nil {
		return nil, err
	}

	result, err := driverStmt.ExecContext(ctx, args)
	if err != nil {
		return nil, err
	}

	var lastInsertID int64
	if !c.noLastInsertID {
		if lastInsertID, err = result.LastInsertId(); err != nil {
			if !strings.Contains(err.Error(), "not supported") {
				return nil, err
			}
			c.noLastInsertID = true
		}
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}

	response := protocol.NewResponseExec(lastInsertID, rowsAffected)

	return response, nil
}

// Handle a request of type QUERY.
func (c *gatewayConn) handleQuery(ctx context.Context, request *protocol.RequestQuery) (*protocol.Response, error) {
	if len(c.txs) != 1 {
		return nil, fmt.Errorf("not in a transaction")
	}
	driverStmt, ok := c.stmts[request.Id]
	if !ok {
		return nil, fmt.Errorf("no prepared statement with ID %d", request.Id)
	}

	args, err := protocol.ToDriverValues(request.Args)
	if err != nil {
		return nil, err
	}

	driverRows, err := driverStmt.QueryContext(ctx, args)
	if err != nil {
		return nil, err
	}

	c.serial++
	c.rows[c.serial] = driverRows

	return protocol.NewResponseQuery(c.serial, driverRows.Columns()), nil
}

// Handle a request of type NEXT.
func (c *gatewayConn) handleNext(ctx context.Context, request *protocol.RequestNext) (*protocol.Response, error) {
	if len(c.txs) != 1 {
		return nil, fmt.Errorf("not in a transaction")
	}
	driverRows, ok := c.rows[request.Id]
	if !ok {
		return nil, fmt.Errorf("no rows with ID %d", request.Id)
	}

	dest := make([]driver.Value, int(request.Len))
	err := driverRows.Next(dest)
	if err == io.EOF {
		return protocol.NewResponseNext(true, nil), nil
	}

	if err != nil {
		return nil, err
	}

	values, err := protocol.FromDriverValues(dest)
	if err != nil {
		return nil, err
	}

	return protocol.NewResponseNext(false, values), nil
}

// Handle a request of type COLUMN_TYPE_SCAN_TYPE.
func (c *gatewayConn) handleColumnTypeScanType(ctx context.Context, request *protocol.RequestColumnTypeScanType) (*protocol.Response, error) {
	if len(c.txs) != 1 {
		return nil, fmt.Errorf("not in a transaction")
	}
	driverRows, ok := c.rows[request.Id]
	if !ok {
		return nil, fmt.Errorf("no rows with ID %d", request.Id)
	}

	code := protocol.ValueCode_BYTES
	typeScanner, ok := driverRows.(driver.RowsColumnTypeScanType)
	if ok {
		typ := typeScanner.ColumnTypeScanType(int(request.Column))
		code = protocol.ToValueCode(typ)
	}

	return protocol.NewResponseColumnTypeScanType(code), nil
}

// Handle a request of type COLUMN_TYPE_DATABASE_TYPE_NAME.
func (c *gatewayConn) handleColumnTypeDatabaseTypeName(ctx context.Context, request *protocol.RequestColumnTypeDatabaseTypeName) (*protocol.Response, error) {
	if len(c.txs) != 1 {
		return nil, fmt.Errorf("not in a transaction")
	}
	driverRows, ok := c.rows[request.Id]
	if !ok {
		return nil, fmt.Errorf("no rows with ID %d", request.Id)
	}

	name := ""
	nameScanner, ok := driverRows.(driver.RowsColumnTypeDatabaseTypeName)
	if ok {
		name = nameScanner.ColumnTypeDatabaseTypeName(int(request.Column))
	}

	return protocol.NewResponseColumnTypeDatabaseTypeName(name), nil
}

// Handle a request of type ROWS_CLOSE.
func (c *gatewayConn) handleRowsClose(ctx context.Context, request *protocol.RequestRowsClose) (*protocol.Response, error) {
	if len(c.txs) != 1 {
		return nil, fmt.Errorf("not in a transaction")
	}
	driverRows, ok := c.rows[request.Id]
	if !ok {
		return nil, fmt.Errorf("no rows with ID %d", request.Id)
	}
	delete(c.rows, request.Id)

	if err := driverRows.Close(); err != nil {
		return nil, err
	}

	response := protocol.NewResponseRowsClose()
	return response, nil
}

// Handle a request of type STMT_CLOSE.
func (c *gatewayConn) handleStmtClose(ctx context.Context, request *protocol.RequestStmtClose) (*protocol.Response, error) {
	if len(c.txs) != 1 {
		return nil, fmt.Errorf("not in a transaction")
	}
	driverStmt, ok := c.stmts[request.Id]
	if !ok {
		return nil, fmt.Errorf("no prepared statement with ID %d", request.Id)
	}
	delete(c.stmts, request.Id)

	if err := driverStmt.Close(); err != nil {
		return nil, err
	}

	response := protocol.NewResponseStmtClose()
	return response, nil
}

// Handle a request of type BEGIN.
func (c *gatewayConn) handleBegin(ctx context.Context, request *protocol.RequestBegin) (*protocol.Response, error) {
	c.mu.Lock()
	if len(c.txs) != 0 {
		c.mu.Unlock()
		return nil, fmt.Errorf("transaction in progress")
	}
	driverTx, err := c.driverConn.BeginTx(ctx, driver.TxOptions{})
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	c.serial++
	c.txs[c.serial] = driverTx
	c.txCh = make(chan struct{})

	// Kill the transaction after a fixed timeout.
	go func(tx driver.Tx, serial int64, ch chan struct{}) {
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			c.rollback()
		}
	}(driverTx, c.serial, c.txCh)
	return protocol.NewResponseBegin(c.serial), nil
}

// Handle a request of type COMMIT.
func (c *gatewayConn) handleCommit(ctx context.Context, request *protocol.RequestCommit) (*protocol.Response, error) {
	if len(c.txs) != 1 {
		return nil, fmt.Errorf("not in a transaction")
	}

	driverTx, ok := c.txs[request.Id]
	if !ok {
		return nil, fmt.Errorf("no transaction with ID %d", request.Id)
	}
	delete(c.txs, request.Id)

	defer func() {
		c.abort()
		close(c.txCh)
		c.mu.Unlock()
	}()

	if err := driverTx.Commit(); err != nil {
		return nil, err
	}

	response := protocol.NewResponseCommit()
	return response, nil
}

// Handle a request of type ROLLBACK.
func (c *gatewayConn) handleRollback(ctx context.Context, request *protocol.RequestRollback) (*protocol.Response, error) {
	if len(c.txs) != 1 {
		return nil, fmt.Errorf("not in a transaction")
	}

	driverTx, ok := c.txs[request.Id]
	if !ok {
		c.abort()
		return nil, fmt.Errorf("no transaction with ID %d", request.Id)
	}
	delete(c.txs, request.Id)

	defer func() {
		c.abort()
		close(c.txCh)
		c.mu.Unlock()
	}()

	if err := driverTx.Rollback(); err != nil {
		return nil, err
	}

	response := protocol.NewResponseRollback()
	return response, nil
}

func (c *gatewayConn) rollback() {
	if len(c.txs) > 1 {
		panic("multiple transactions detected")
	}
	if len(c.txs) == 0 {
		// nothing to do
		return
	}
	for id, tx := range c.txs {
		c.abort()
		delete(c.txs, id)
		tx.Rollback()
		close(c.txCh)
		c.mu.Unlock()
	}
}

// Handle a request of type CLOSE.
func (c *gatewayConn) handleClose(ctx context.Context, request *protocol.RequestClose) (*protocol.Response, error) {
	if err := c.Close(); err != nil {
		return nil, err
	}
	response := protocol.NewResponseClose()
	return response, nil
}

func (c *gatewayConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.refcount--

	if c.refcount == 0 {
		c.rollback()
		conn := c.driverConn
		c.driverConn = nil
		c.txs = nil
		c.stmts = nil
		c.rows = nil
		if err := conn.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Handle a request of type CONN_EXEC.
func (c *gatewayConn) handleConnExec(ctx context.Context, request *protocol.RequestConnExec) (*protocol.Response, error) {
	if len(c.txs) != 1 {
		return nil, fmt.Errorf("not in a transaction")
	}
	args, err := protocol.ToDriverValues(request.Args)
	if err != nil {
		c.abort()
		return nil, err
	}

	var result driver.Result
	if execer, ok := c.driverConn.(driver.ExecerContext); ok {
		result, err = execer.ExecContext(ctx, request.Query, args)
	} else {
		stmt, prepErr := c.driverConn.PrepareContext(ctx, request.Query)
		if prepErr != nil {
			c.abort()
			return nil, fmt.Errorf("prepare %s: %w", request.Query, prepErr)
		}
		defer stmt.Close()
		result, err = stmt.(driver.StmtExecContext).ExecContext(ctx, args)
	}
	if err != nil {
		c.abort()
		return nil, err
	}

	lastInsertID, err := result.LastInsertId()
	if err != nil {
		c.abort()
		return nil, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		c.abort()
		return nil, err
	}

	response := protocol.NewResponseExec(lastInsertID, rowsAffected)

	return response, nil
}
