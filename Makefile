.PHONY: build
build: bin/astprinter

.PHONY: bin/astprinter
bin/astprinter: vendor
	go build -o bin/astprinter cmd/astprinter/main.go

vendor: Gopkg.toml Gopkg.lock
	dep ensure

.PHONY: test
test: vendor
	go test ./... -cover -count=1 -v

