// Copyright 2017, 2018 Canonical Ltd.
//
// SPDX-License-Identifier: Apache-2.0

package grpcsql

import (
	"database/sql/driver"

	"github.com/godror/go-grpc-sql/internal/protocol"
	"google.golang.org/grpc"
)

// NewServer is a convenience for creating a gRPC server with a registered SQL
// gateway backed by the given driver.
func NewServer(driver driver.DriverContext, opt ...grpc.ServerOption) *grpc.Server {
	gateway := NewGateway(driver)
	server := grpc.NewServer(opt...)
	protocol.RegisterSQLServer(server, gateway)
	return server
}
