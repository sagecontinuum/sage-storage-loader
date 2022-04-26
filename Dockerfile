FROM golang:1.18 AS builder
WORKDIR /build
COPY . .
RUN go build -ldflags="-s -w" -o sage-storage-loader

FROM scratch
COPY --from=builder /build/sage-storage-loader /sage-storage-loader
ENTRYPOINT [ "/sage-storage-loader" ]
