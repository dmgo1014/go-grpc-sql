module github.com/godror/go-grpc-sql

go 1.15

require (
	github.com/godror/godror v0.23.1-0.20210103152835-07c626fd4c49
	github.com/golang/protobuf v1.4.3
	github.com/mpvl/subtest v0.0.0-20160608141506-f6e4cfd4b9ea
	github.com/stretchr/testify v1.6.1
	golang.org/x/net v0.0.0-20201224014010-6772e930b67b
	google.golang.org/grpc v1.34.0
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.0.1 // indirect
	google.golang.org/protobuf v1.25.0
)

//replace github.com/godror/godror => ../godror
