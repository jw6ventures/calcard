# Build stage
FROM golang:1.24 AS builder
WORKDIR /app
COPY go.mod .
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o calcard ./cmd/server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y curl ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    useradd -r -u 1000 -m -s /bin/false calcard
ENV APP_LISTEN_ADDR=":8080"
EXPOSE 8080
WORKDIR /app
COPY --from=builder /app/calcard /app/calcard
COPY --from=builder /app/db.sql /app/db.sql
COPY --from=builder /app/migrations /app/migrations
RUN chmod +x /app/calcard
USER calcard
ENTRYPOINT ["/app/calcard"]
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8080/healthz || exit 1
