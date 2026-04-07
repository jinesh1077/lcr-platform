FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download 2>/dev/null || true
COPY . .
RUN go mod tidy && CGO_ENABLED=0 go build -o /telemetry ./cmd/telemetry

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /telemetry /telemetry
EXPOSE 8082
CMD ["/telemetry"]
