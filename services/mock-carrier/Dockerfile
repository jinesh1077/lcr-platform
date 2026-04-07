FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY main.go .
RUN go mod tidy && CGO_ENABLED=0 go build -o /mock-carrier .

FROM alpine:3.20
COPY --from=builder /mock-carrier /mock-carrier
EXPOSE 8083
CMD ["/mock-carrier"]
