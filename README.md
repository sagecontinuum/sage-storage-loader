# Sage Storage Loader

This service loads files staged in a directory (on Beehive) into OSN.

The expected data flow is:

```txt
          rsync uploads                   sage storage loader
[ node ] --------------> [ staging dir ] --------------------> [ osn ]
```

# Architecture

```txt
       scanner process walks staging dir
       and puts ready uploads into channel

                                          +--> [ worker ] --> [     ]
[ staging dir ] -----------> [|||] -------+--> [ worker ] --> [ osn ]
                                          +--> [ worker ] --> [     ]

                                        workers upload files to osn
                                        and clean them up afterwards
```

# Development

MinIO instance from [https://github.com/sagecontinuum/sage-storage-api](https://github.com/sagecontinuum/sage-storage-api) can be used for testing.


```console
docker build -t waggle/sage-uploader .
docker run -ti --rm --env-file ./.env -v ${PWD}/temp:/data  waggle/sage-uploader
```

# Dev RMQ  ( not used anymore ? )

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

