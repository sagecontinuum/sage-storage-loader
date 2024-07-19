FROM golang:1.19-alpine AS builder
WORKDIR /build
COPY . .
RUN CGO_ENABLED=0 go build

FROM alpine:3.15
COPY --from=builder /build/sage-storage-loader /usr/bin/sage-storage-loader
ENTRYPOINT [ "/usr/bin/sage-storage-loader" ]
