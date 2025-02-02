// Copyright 2021 The Godror Authors.
// Copyright 2017, 2018 Canonical Ltd.
//
// SPDX-License-Identifier: Apache-2.0

package grpcsql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"

	"github.com/godror/go-grpc-sql/internal/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ = (driver.Conn)((*Conn)(nil))
var _ = (driver.ConnBeginTx)((*Conn)(nil))

// Conn wraps a connection to a gRPC SQL gateway.
type Conn struct {
	grpcConn       *grpc.ClientConn
	grpcConnClient protocol.SQL_ConnClient
	grpcConnDoomed bool // Whether the connection should be considered broken.
}

// Prepare returns a prepared statement, bound to this connection.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	response, err := c.exec(protocol.NewRequestPrepare(query))
	if err != nil {
		return nil, err
	}
	stmt := &Stmt{
		conn:     c,
		id:       response.Prepare().Id,
		numInput: int(response.Prepare().NumInput),
	}
	return stmt, nil
}

// Exec may return ErrSkip.
//
// Deprecated: Drivers should implement ExecerContext instead (or additionally).
func (c *Conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	values, err := protocol.FromDriverValues(args)
	if err != nil {
		return nil, err
	}

	response, err := c.exec(protocol.NewRequestConnExec(query, values))
	if err != nil {
		return nil, err
	}

	result := &Result{
		lastInsertID: response.Exec().LastInsertId,
		rowsAffected: response.Exec().RowsAffected,
	}
	return result, nil
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
func (c *Conn) Close() error {
	if _, err := c.exec(protocol.NewRequestClose()); err != nil {
		return err
	}
	return c.grpcConn.Close()
}

func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx starts and returns a new transaction.
func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	response, err := c.exec(protocol.NewRequestBegin(opts))
	if err != nil {
		return nil, err
	}
	tx := &Tx{
		conn: c,
		id:   response.Begin().Id,
	}
	return tx, nil
}

// Execute a request and waits for the response.
func (c *Conn) exec(request *protocol.Request) (*protocol.Response, error) {
	if c.grpcConnDoomed {
		// This means that we previously failed because of a connection
		// error, so we want to just fail again (since the sql package
		// retries ErrBadConn).
		return nil, driver.ErrBadConn
	}

	if err := c.grpcConnClient.Send(request); err != nil {
		return nil, c.errorf(err, "gRPC could not send %s request", request.Code)
	}

	response, err := c.grpcConnClient.Recv()
	if err != nil {
		return nil, c.errorf(err, "gRPC %s response error", request.Code)
	}
	switch response.Code {
	case protocol.RequestCode_SQL_ERROR:
		err := response.SQLError()
		return nil, err
	}
	return response, nil
}

// If the given error is due to the gRPC endpoint being unavailable, return
// ErrBadConn and mark the connection as doomed, otherwise return the original error.
func (c *Conn) errorf(err error, format string, v ...interface{}) error {
	if isBadConn(err) {
		c.grpcConnDoomed = true
		return driver.ErrBadConn
	}
	return fmt.Errorf(format+": %w", append(v, err)...)
}

func isBadConn(err error) bool {
	status, ok := status.FromError(err)
	if ok {
		code := status.Code()
		if code == codes.Canceled || code == codes.Unavailable {
			return true
		}
		// FIXME: this look like a spurious error which gets generated
		//        only by the http test server of Go >= 1.9. Still, we
		//        handle it in this ad-hoc way because when it happens
		//        it simply means that the other end is down.
		if code == codes.Internal && status.Message() == grpcNoErrorMessage {
			return true
		}
	}
	return errors.Is(err, io.EOF)
}

const grpcNoErrorMessage = "stream terminated by RST_STREAM with error code: NO_ERROR"
