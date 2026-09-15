# ---- Build stage ----
FROM golang:1.25-alpine AS build
WORKDIR /src

# Cache module downloads first.
COPY go.mod ./
RUN go mod download

# Copy source and build a static binary.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/memcached ./cmd/server

# ---- Runtime stage ----
FROM alpine:3.20
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 memcache
USER memcache
COPY --from=build /out/memcached /usr/local/bin/memcached

ENV MEMCACHE_ADDR=":11211" \
    MEMCACHE_MAX_MB="64"

EXPOSE 11211
ENTRYPOINT ["/usr/local/bin/memcached"]
