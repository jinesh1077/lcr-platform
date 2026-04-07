FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download 2>/dev/null || true
COPY . .
RUN go mod tidy && CGO_ENABLED=0 go build -o /ingestion ./cmd/ingestion && CGO_ENABLED=0 go build -o /trie-builder ./cmd/trie-builder

FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget
WORKDIR /app
COPY --from=builder /ingestion /trie-builder ./
COPY migrations ./migrations
EXPOSE 8080
CMD ["./ingestion"]
