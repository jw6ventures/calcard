# Build stage
FROM golang:1.24-alpine AS builder
RUN apk add --no-cache \
  ca-certificates \
  git
RUN update-ca-certificates
WORKDIR /src
COPY go.mod go.sum .
RUN go mod download
COPY . .
RUN mkdir /app && cp -r db.sql migrations /app/
RUN CGO_ENABLED=0 go build -o /app/calcard ./cmd/server

FROM scratch
USER 1000:1000
ENV APP_LISTEN_ADDR=":8080"
WORKDIR /app
COPY --from=builder --chown=1000:1000 /app /app
COPY --from=builder --chown=1000:1000 /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
ENTRYPOINT ["/app/calcard"]
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD ["/app/calcard", "healthcheck"]
