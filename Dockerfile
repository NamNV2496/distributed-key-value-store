# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Install dependencies
RUN apk add --no-cache git

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build main binary (for node1, node2, node3)
RUN CGO_ENABLED=0 GOOS=linux go build -o /raft-redis .

# Build the client service (example/client.go)
RUN CGO_ENABLED=0 GOOS=linux go build -o /client-service ./example/client.go

# Final stage
FROM alpine:latest

RUN apk add --no-cache ca-certificates curl

WORKDIR /app

# Copy both binaries from builder
COPY --from=builder /raft-redis .
COPY --from=builder /client-service .

EXPOSE 5000 8000

CMD ["./raft-redis"]