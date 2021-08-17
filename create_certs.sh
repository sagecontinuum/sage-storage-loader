#!/bin/bash
set -e
set -x

# create CA: (cacert.pem , cakey.pem)
CA_DIR="temp/ca"
mkdir -p ${CA_DIR}
openssl req -x509 -nodes -newkey rsa:4096 -keyout ${CA_DIR}/cakey.pem -out ${CA_DIR}/cacert.pem -sha256 -days 36500 -subj /CN=beekeeper

## rabbitmq:


RMQ_DIR="temp/rmq"
mkdir -p ${RMQ_DIR}
#create CSR (from new key!?)
openssl req -new -nodes -newkey rsa:4096 -keyout ${RMQ_DIR}/beehive-rabbitmq.key.pem -out ${RMQ_DIR}/beehive-rabbitmq.csr.pem -subj /CN=host.docker.internal #beehive-rabbitmq
# sign
openssl x509 -req -extfile <(printf "subjectAltName=DNS:host.docker.internal,DNS:beehive-rabbitmq") -in ${RMQ_DIR}/beehive-rabbitmq.csr.pem -out ${RMQ_DIR}/beehive-rabbitmq.cert.pem -CAkey ${CA_DIR}/cakey.pem -CA ${CA_DIR}/cacert.pem -CAcreateserial -sha256 -days 365

# create key and cert for object-store-uploader
CLIENT_CERT_DIR="temp/clientcert"
mkdir -p ${CLIENT_CERT_DIR}
openssl req -new -nodes -newkey rsa:4096 -keyout ${CLIENT_CERT_DIR}/object-store-uploader.key.pem -out ${CLIENT_CERT_DIR}/object-store-uploader.csr.pem -subj /CN=object-store-uploader
openssl x509 -req -in ${CLIENT_CERT_DIR}/object-store-uploader.csr.pem -out ${CLIENT_CERT_DIR}/object-store-uploader.cert.pem -CAkey ${CA_DIR}/cakey.pem -CA ${CA_DIR}/cacert.pem -CAcreateserial -sha256 -days 365
