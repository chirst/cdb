run:
	go run .

test:
	go test -v ./...

test-long:
	LONG_TEST=1 go test -v -run ^TestInsertAndSelectMillions github.com/chirst/cdb/db

build:
	go build -o cdb main.go

buildc:
	go build -o cdb.so -buildmode=c-shared main.go

testc:
	cc -o testc.out test.c ./cdb.so && ./testc.out

buildtestc:
	make buildc && make testc
