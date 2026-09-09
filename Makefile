.PHONY: run bench test fmt vet

run:
	go run ./cmd/dispatch

bench:
	go run ./cmd/loadgen -clients 1000 -duration 30s

fmt:
	gofmt -w .

vet:
	go vet ./...

test:
	go test ./...
