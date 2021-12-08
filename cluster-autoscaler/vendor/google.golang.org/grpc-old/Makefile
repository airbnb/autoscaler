all: vet test testrace

build: deps
	go build google.golang.org/grpc-old/...

clean:
	go clean -i google.golang.org/grpc-old/...

deps:
	go get -d -v google.golang.org/grpc-old/...

proto:
	@ if ! which protoc > /dev/null; then \
		echo "error: protoc not installed" >&2; \
		exit 1; \
	fi
	go generate google.golang.org/grpc-old/...

test: testdeps
	go test -cpu 1,4 -timeout 7m google.golang.org/grpc-old/...

testappengine: testappenginedeps
	goapp test -cpu 1,4 -timeout 7m google.golang.org/grpc-old/...

testappenginedeps:
	goapp get -d -v -t -tags 'appengine appenginevm' google.golang.org/grpc-old/...

testdeps:
	go get -d -v -t google.golang.org/grpc-old/...

testrace: testdeps
	go test -race -cpu 1,4 -timeout 7m google.golang.org/grpc-old/...

updatedeps:
	go get -d -v -u -f google.golang.org/grpc-old/...

updatetestdeps:
	go get -d -v -t -u -f google.golang.org/grpc-old/...

vet: vetdeps
	./vet.sh

vetdeps:
	./vet.sh -install

.PHONY: \
	all \
	build \
	clean \
	deps \
	proto \
	test \
	testappengine \
	testappenginedeps \
	testdeps \
	testrace \
	updatedeps \
	updatetestdeps \
	vet \
	vetdeps
