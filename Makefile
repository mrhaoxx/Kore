GOBIN := $(shell pwd)/bin
CONTROLLER_GEN := $(GOBIN)/controller-gen

.PHONY: build test fmt vet generate manifests

build:
	go build ./...

test:
	go test ./... -count=1

fmt:
	gofmt -l -w .

vet:
	go vet ./...

$(CONTROLLER_GEN):
	GOBIN=$(GOBIN) go install sigs.k8s.io/controller-tools/cmd/controller-gen@v0.19.0

generate: $(CONTROLLER_GEN)
	$(CONTROLLER_GEN) object paths=./pkg/apis/...

manifests: $(CONTROLLER_GEN)
	$(CONTROLLER_GEN) crd paths=./pkg/apis/... output:crd:dir=deploy/crd
