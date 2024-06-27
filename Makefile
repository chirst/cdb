run:
	go run .

test:
	go test -v ./...

build:
	go build -o cdb main.go
