go-grpc-sql [![Go Report Card](https://goreportcard.com/badge/github.com/godror/go-grpc-sql)](https://goreportcard.com/report/github.com/godror/go-grpc-sql) [![GoDoc](https://pkg.go.dev/github.com/godror/go-grpc-sql?status.svg)](https://godoc.org/github.com/godror/go-grpc-sql)
=========

This repository provides the `grpcsql` package, which can be used
to expose a Go SQL `driver.Driver` instance over gRPC.

For example you could cross-compile a [Godror](https://github.com/godror/godror) gateway
for every possible architecture (using [xgo](https://github.com/karalabe/xgo)), 
and use that gateway everywhere, instead of cross-compiling your app with cgo.

Documentation
==============

The documentation for this package can be found on [Godoc](http://pkg.go.dev/github.com/godror/go-grpc-sql).
