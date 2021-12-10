all: vet test testrace

build:
	go build -new/...

clean:
	go clean -i google.golang.org/grpc/v2/...

deps:
	GO111MODULE=on go get -d -v google.golang.org/grpc/v2/...

proto:
	@ if ! which protoc > /dev/null; then \
		echo "error: protoc not installed" >&2; \
		exit 1; \
	fi
	go generate google.golang.org/grpc/v2/...

test:
	go test -cpu 1,4 -timeout 7m google.golang.org/grpc/v2/...

testsubmodule:
	cd security/advancedtls && go test -cpu 1,4 -timeout 7m google.golang.org/grpc/v2/security/advancedtls/...
	cd security/authorization && go test -cpu 1,4 -timeout 7m google.golang.org/grpc/v2/security/authorization/...

testrace:
	go test -race -cpu 1,4 -timeout 7m google.golang.org/grpc/v2/...

testdeps:
	GO111MODULE=on go get -d -v -t google.golang.org/grpc/v2/...

vet: vetdeps
	./vet.sh

vetdeps:
	./vet.sh -install

.PHONY: \
	all \
	build \
	clean \
	proto \
	test \
	testappengine \
	testappenginedeps \
	testrace \
	vet \
	vetdeps
