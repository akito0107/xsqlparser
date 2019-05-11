SHELL := PATH="$(PWD)/tools/bin:$(PATH)" $(SHELL)

.PHONY: build
build: bin/astprinter

.PHONY: bin/astprinter
bin/astprinter: vendor generate
	go build -o bin/astprinter cmd/astprinter/main.go

.PHONY: tools/bin/genmark
tools/bin/genmark:
	go build -o tools/bin/genmark tools/genmark/main.go

.PHONY: generate
generate: tools/bin/genmark
	go generate ./...

vendor: Gopkg.toml Gopkg.lock
	dep ensure

.PHONY: test
test: vendor
	go test ./... -cover -count=1 -v

.PHONY: install
install: vendor
	go install ./cmd/...

