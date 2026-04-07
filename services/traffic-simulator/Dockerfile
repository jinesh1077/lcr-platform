FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY main.go .
RUN go mod tidy && CGO_ENABLED=0 go build -o /simulator .

FROM alpine:3.20
COPY --from=builder /simulator /simulator
CMD ["/simulator"]
