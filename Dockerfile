

# docker build -t waggle/sage-uploader .
# docker run -ti  waggle/sage-uploader
# for development:
# docker run -ti --rm --env-file ./.env -v ${PWD}:/go/src/app/ -v ${PWD}/temp/rmq/:/tls/ -v ${PWD}/temp/ca/:/ca/ --entrypoint /bin/bash waggle/sage-uploader

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


ENTRYPOINT [ "./uploader" ]
CMD []
