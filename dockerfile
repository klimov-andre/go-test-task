FROM golang:latest
RUN mkdir /go-test-task
WORKDIR /go-test-task
COPY ./ /go-test-task
ENTRYPOINT go run ./main.go
EXPOSE 8080