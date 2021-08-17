

# docker build -t waggle/sage-uploader .
# docker run -ti  waggle/sage-uploader
# for development:
# docker run -ti -v ${PWD}:/go/src/app/ -v ${PWD}/temp/rmq/:/tls/ -v ${PWD}/temp/ca/:/ca/ --entrypoint /bin/bash waggle/sage-uploader

FROM golang:1.16


RUN apt-get update && apt-get install -y inotify-tools

WORKDIR /go/src/app


COPY *.go ./go.*  /go/src/app/
ADD rwmutex /go/src/app/rwmutex

# create go.mod
#> go mod init github.com/sagecontinuum/sage-storage-loader
# update go.mod
#> go mod tidy
#RUN go get -u gotest.tools/gotestsum


# got get ... will create/append go.sum

RUN go build -o uploader

ENV s3Endpoint=http://host.docker.internal:9001
ENV s3accessKeyID=minio
ENV s3secretAccessKey=minio123
ENV s3bucket=sage

ENV rabbitmq_host=host.docker.internal
ENV rabbitmq_port=5671
ENV rabbitmq_user=object-store-uploader
ENV rabbitmq_password=test
ENV rabbitmq_exchange="waggle.msg"
ENV rabbitmq_queue="influx-messages"
ENV rabbitmq_routingkey="#"

ENV rabbitmq_cacert_file=/ca/cacert.pem
ENV rabbitmq_cert_file=/tls/beehive-rabbitmq.cert.pem
ENV rabbitmq_key_file=/tls/beehive-rabbitmq.key.pem


ENTRYPOINT [ "./uploader" ]
CMD []
