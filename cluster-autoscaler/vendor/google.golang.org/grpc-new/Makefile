all: vet test testrace

build:
	go build -new/...

clean:
	go clean -i google.golang.org/grpc-new/...

deps:
	GO111MODULE=on go get -d -v google.golang.org/grpc-new/...

proto:
	@ if ! which protoc > /dev/null; then \
		echo "error: protoc not installed" >&2; \
		exit 1; \
	fi
	go generate google.golang.org/grpc-new/...

test:
	go test -cpu 1,4 -timeout 7m google.golang.org/grpc-new/...

testsubmodule:
	cd security/advancedtls && go test -cpu 1,4 -timeout 7m google.golang.org/grpc-new/security/advancedtls/...
	cd security/authorization && go test -cpu 1,4 -timeout 7m google.golang.org/grpc-new/security/authorization/...

testrace:
	go test -race -cpu 1,4 -timeout 7m google.golang.org/grpc-new/...

testdeps:
	GO111MODULE=on go get -d -v -t google.golang.org/grpc-new/...

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
