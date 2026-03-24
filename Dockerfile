# Build stage
FROM golang:1.24-alpine AS builder
RUN apk add --no-cache ca-certificates
RUN update-ca-certificates
WORKDIR /app
COPY go.mod go.sum .
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o calcard ./cmd/server

FROM scratch
USER 1000:1000
ENV APP_LISTEN_ADDR=":8080"
WORKDIR /app
COPY --from=builder /app/calcard /app/calcard
COPY --from=builder /app/db.sql /app/db.sql
COPY --from=builder /app/migrations /app/migrations
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
ENTRYPOINT ["/app/calcard"]
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD ["/app/calcard", "healthcheck"]
