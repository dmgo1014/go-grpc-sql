// Copyright 2021 The Godror Authors.
// Copyright 2017, 2018 Canonical Ltd.
//
// SPDX-License-Identifier: Apache-2.0

package grpcsql_test

import (
	"database/sql/driver"
	"os"
	"testing"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	grpcsql "github.com/godror/go-grpc-sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testDSN string

func init() {
	testDSN = os.Getenv("TEST_DSN")
	if testDSN == "" {
		panic("Set TEST_DSN environment variable to a test database connection string!")
	}
}

// Open a new gRPC connection.
func TestDriver_Open(t *testing.T) {
	driver, cleanup := newDriver()
	defer cleanup()

	conn, err := driver.Open(testDSN)
	require.NoError(t, err)
	defer conn.Close()
}

// Create a transaction and commit it.
func TestDriver_TxCommit(t *testing.T) {
	drv, cleanup := newDriver()
	defer cleanup()

	conn, err := drv.Open(testDSN)
	require.NoError(t, err)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	tx, err := conn.(driver.ConnBeginTx).BeginTx(ctx, driver.TxOptions{})
	require.NoError(t, err)
	assert.NoError(t, tx.Commit())
}

// Open a new gRPC connection.
func TestDriver_BadConn(t *testing.T) {
	drv, cleanup := newDriver()

	conn, err := drv.Open(testDSN)
	assert.NoError(t, err)
	defer conn.Close()

	// Shutdown the server to interrupt the gRPC connection.
	cleanup()

	stmt, err := conn.Prepare("SELECT * FROM sqlite_master")
	assert.Nil(t, stmt)
	assert.Equal(t, driver.ErrBadConn, err)
}

// Possible failure modes of Driver.Open().
func TestDriver_OpenError(t *testing.T) {
	cases := []struct {
		title  string
		dialer grpcsql.Dialer
		err    string
	}{
		{
			"gRPC connection failed",
			func() (*grpc.ClientConn, error) {
				return grpc.Dial("1.2.3.4", grpc.WithInsecure())
			},
			"gRPC conn method failed",
		},
	}
	for _, c := range cases {
		t.Run(c.title, func(t *testing.T) {
			driver := grpcsql.NewDriver(c.dialer)
			db, err := driver.Open(testDSN)
			if db != nil {
				db.Close()
			}
			require.NotNil(t, err)
			assert.Contains(t, err.Error(), c.err)
		})
	}
}

// Return a new Driver instance configured to connect to a test gRPC server.
func newDriver() (*grpcsql.Driver, func()) {
	server, address := newGatewayServer()
	dialer := func() (*grpc.ClientConn, error) {
		return grpc.Dial(address, grpc.WithInsecure())
	}
	driver := grpcsql.NewDriver(dialer)
	cleanup := func() { server.Stop() }
	return driver, cleanup
}
