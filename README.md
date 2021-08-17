# sage-storage-loader
Load files from filesystem into SAGE object store


This uploader load files from the beehive filesystem to the SAGE object store. Nodes upload large files via rsync to beehive and the the files are cached on the beehive filesystem. This loader checks the upload directrory for new files and uploads them (using mulitiple worker processes) to buckets in the SAGE object store.

The Sage object store API is only used to create buckets. For performance reasons, this uploader by-passes Sage object store API and then uploads files directly into the underlying S3 store. (Normal sage users do not have the permission to upload to the backend S3, they can only upload to the Sage API)









# Dev RMQ

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

