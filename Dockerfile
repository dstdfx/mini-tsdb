# Dockerfile
FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY . .
RUN go build -o mini-tsdb .

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/mini-tsdb .
EXPOSE 9201 
CMD ["./mini-tsdb"]
