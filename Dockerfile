FROM golang:1.18 AS builder
WORKDIR /sage-storage-loader
COPY . .
RUN go build -ldflags="-s -w"

# using alpine image's root certificates for https
FROM alpine:3.15
# FROM scratch
COPY --from=builder /sage-storage-loader/sage-storage-loader /usr/bin/sage-storage-loader
ENTRYPOINT [ "/usr/bin/sage-storage-loader" ]
