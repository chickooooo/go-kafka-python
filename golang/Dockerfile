# Stage 1: Build the Go binary
FROM golang:1.25-alpine AS builder

# Set Go environment variables
# Disable C linking, Linux OS, x86_64 CPU
ENV CGO_ENABLED=0 GOOS=linux GOARCH=amd64

# Create working directory
WORKDIR /app

# Copy go.mod and go.sum first for caching dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source code
COPY . .

# Build the binary
# -ldflags "-s -w": omit debug information and symbol tables.
RUN go build -ldflags="-s -w" -o myapp .

# Stage 2: Minimal runtime image
FROM scratch

# Copy the statically compiled Go binary
COPY --from=builder /app/myapp /myapp

# Set entrypoint
ENTRYPOINT ["/myapp"]
