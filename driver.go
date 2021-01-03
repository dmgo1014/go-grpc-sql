package grpcsql

import (
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/godror/go-grpc-sql/internal/protocol"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Driver implements the database/sql/driver interface and executes the
// relevant statements over gRPC.
type Driver struct {
	dialer Dialer
}

// NewDriver creates a new gRPC SQL driver for creating connections to backend
// gateways.
func NewDriver(dialer Dialer) *Driver {
	return &Driver{
		dialer: dialer,
	}
}

// Dialer is a function that can create a gRPC connection.
type Dialer func() (conn *grpc.ClientConn, err error)

// Open a new connection against a gRPC SQL server.
//
// To establish the gRPC connection, the dialer passed to NewDriver() will
// used.
//
// The given data source name must be one that the driver attached to the
//remote Gateway can understand.
func (d *Driver) Open(name string) (driver.Conn, error) {
	conn, err := dial(d.dialer, name)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// Create a new connection to a gRPC endpoint.
func dial(dialer Dialer, name string) (*Conn, error) {
	grpcConn, err := dialer()
	if err != nil {
		return nil, fmt.Errorf("gRPC grpcConnection failed: %w", err)
	}

	// TODO: make the number of retries and timeout configurable
	var conn *Conn
	var lastErr error
	for i := 0; i < 3; i++ {
		grpcClient := protocol.NewSQLClient(grpcConn)
		grpcConnClient, err := grpcClient.Conn(context.Background())
		if err == nil {
			lastErr = nil
			conn = &Conn{
				grpcConn:       grpcConn,
				grpcConnClient: grpcConnClient,
			}
			break
		}
		if status.Code(err) != codes.Unavailable {
			return nil, fmt.Errorf("gRPC conn method failed: %w", err)
		}
		lastErr = err
		time.Sleep(time.Duration(i) * time.Second)
	}
	if lastErr != nil {
		return nil, fmt.Errorf("gRPC conn method failed: %w", lastErr)
	}

	if _, err := conn.exec(protocol.NewRequestOpen(name)); err != nil {
		return nil, fmt.Errorf("gRPC could not send open request: %w", err)
	}

	return conn, nil
}
