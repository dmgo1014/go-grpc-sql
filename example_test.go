// Copyright 2021 The Godror Authors.
// Copyright 2017, 2018 Canonical Ltd.
//
// SPDX-License-Identifier: Apache-2.0

package grpcsql_test

import (
	"database/sql"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"

	grpcsql "github.com/godror/go-grpc-sql"
	"github.com/godror/godror"
)

func Example() {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalf("failed to create listener: %v", err)
	}
	server := grpcsql.NewServer(godror.NewDriver())
	go server.Serve(listener)
	defer server.Stop()

	dialer := func() (*grpc.ClientConn, error) {
		return grpc.Dial(listener.Addr().String(), grpc.WithInsecure())
	}
	driver := grpcsql.NewDriver(dialer)
	sql.Register("grpc", driver)

	db, err := sql.Open("grpc", testDSN)
	if err != nil {
		log.Fatalf("failed to create grpc database: %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("failed to create grpc transaction: %v", err)
	}
	defer tx.Rollback()

	const tbl = "test_grpc"
	tx.Exec("DROP TABLE " + tbl)
	defer tx.Exec("DROP TABLE " + tbl)
	if _, err := tx.Exec("CREATE TABLE " + tbl + " (n INTEGER)"); err != nil {
		log.Fatalf("failed to execute create table statement over grpc: %v", err)
	}
	if _, err := tx.Exec("INSERT INTO " + tbl + "(n) VALUES (1)"); err != nil {
		log.Fatalf("failed to execute INSERT statement over grpc: %v", err)
	}

	result, err := tx.Exec("INSERT INTO " + tbl + "(n) VALUES (2)")
	if err != nil {
		log.Fatalf("failed to execute insert statement over grpc: %v", err)
	}

	rows, err := tx.Query("SELECT n FROM " + tbl + " ORDER BY n")
	if err != nil {
		log.Fatalf("failed to select rows over grpc: %s", err)
	}
	types, err := rows.ColumnTypes()
	if len(types) != 1 {
		log.Fatalf("wrong count of column types: %d", len(types))
	}
	name := types[0].DatabaseTypeName()

	if err != nil {
		log.Fatalf("failed to fetch column types over grpc: %s", err)
	}

	numbers := []int{}
	for rows.Next() {
		var n int
		if err := rows.Scan(&n); err != nil {
			log.Fatalf("failed to scan row over grpc: %s", err)
		}
		numbers = append(numbers, n)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("rows error over grpc: %s", err)
	}
	defer rows.Close()

	// Output:
	// 2 <nil>
	// 1 <nil>
	// INTEGER
	// [1 2]
	fmt.Println(result.LastInsertId())
	fmt.Println(result.RowsAffected())
	fmt.Println(name)
	fmt.Println(numbers)
}
