FROM golang:1.18.1 AS builder
WORKDIR /build
COPY . .
RUN go build -ldflags="-s -w" -o /build/sage-storage-loader

# using alpine image's root certificates for https
FROM alpine:3.15.4
WORKDIR /app
COPY --from=builder /build/sage-storage-loader /app/sage-storage-loader
ENTRYPOINT [ "/app/sage-storage-loader" ]
