# sage-storage-loader
Load files from filesystem into SAGE object store


This uploader load files from the beehive filesystem to an S3 bucket. Nodes upload large files via rsync to beehive and the the files are cached on the beehive filesystem. This loader checks the upload directrory for new files and uploads them (using mulitiple worker processes) to S3.



# Development

MinIO instance from [https://github.com/sagecontinuum/sage-storage-api](https://github.com/sagecontinuum/sage-storage-api) can be used for testing.


```console
docker build -t waggle/sage-uploader .
docker run -ti --rm --env-file ./.env -v ${PWD}/temp:/data  waggle/sage-uploader
```

# Dev RMQ  ( not used anymore )

```console
./create_certs.sh
```


```console
docker run -ti --name beehive-rabbitmq --rm -v ${PWD}/temp/ca/:/etc/ca/ -v ${PWD}/temp/rmq/:/etc/tls/ -v ${PWD}/rabbitmq:/etc/rabbitmq  --env RABBITMQ_DEFAULT_USER=test --env RABBITMQ_DEFAULT_PASS=test -p 5671:5671 -p 8081:15672 rabbitmq:3.8.11-management-alpine
```

create exchange and users
```console
./config_test_rabbitmq.sh
```

management UI:
[http://localhost:8081](http://localhost:8081)

