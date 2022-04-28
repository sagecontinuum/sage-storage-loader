FROM golang:1.18-alpine AS builder
WORKDIR /build
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w"

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /build/sage-storage-loader /sage-storage-loader
ENTRYPOINT [ "/sage-storage-loader" ]
