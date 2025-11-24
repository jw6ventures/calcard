# Build stage
FROM golang:1.22 AS builder
WORKDIR /app
COPY go.mod .
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o calcard ./cmd/server

# Runtime stage
FROM gcr.io/distroless/base-debian12
ENV APP_LISTEN_ADDR=":8080"
EXPOSE 8080
COPY --from=builder /app/calcard /calcard
ENTRYPOINT ["/calcard"]
# TODO: include migrations/templates via embedding in future iterations.
