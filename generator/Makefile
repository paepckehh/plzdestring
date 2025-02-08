# generate map

all:
	go run -mod=readonly generate.go
	cd .. 
	gofmt -w -s *.go
	go build
