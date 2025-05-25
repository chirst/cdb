run:
	go run .

test:
	go test -v ./...

test-long:
	LONG_TEST=1 go test -v -run ^TestInsertAndSelectMillions github.com/chirst/cdb/db

build:
	go build -o cdb main.go
