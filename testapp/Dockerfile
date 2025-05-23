FROM golang:1.24 as builder

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . ./

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o test-app main.go

FROM scratch

COPY --from=builder /app/test-app /test-app

EXPOSE 8080

ENTRYPOINT ["/test-app"]
