# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /src

RUN apk add --no-cache git ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/raftd ./cmd/raftd
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/raftctl ./cmd/raftctl

# Runtime stage
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /bin/raftd /usr/local/bin/raftd
COPY --from=builder /bin/raftctl /usr/local/bin/raftctl

EXPOSE 8081 9091

ENTRYPOINT ["/usr/local/bin/raftd"]
