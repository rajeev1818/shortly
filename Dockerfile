FROM golang:1.26-alpine AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o gateway ./cmd/gateway
RUN go build -o shortener ./cmd/shortener

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/gateway .
COPY --from=builder /app/shortener .
COPY migrations/ ./migrations/
