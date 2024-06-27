# Sage Storage Loader

This service loads files staged in a directory (on Beehive) into [OSN](https://www.openstoragenetwork.org/) or [Pelican](https://pelicanplatform.org/). Based on env variable **STORAGE_TYPE** it will use OSN or Pelican.

The expected data flow is:

```txt
          rsync uploads                   sage storage loader
[ node ] --------------> [ staging dir ] --------------------> [ storage ]
```

# Architecture

```txt
       scanner process walks staging dir
       and puts ready uploads into channel

                                          +--> [ worker ] --> [         ]
[ staging dir ] -----------> [|||] -------+--> [ worker ] --> [ storage ]
                                          +--> [ worker ] --> [         ]

                                        workers upload files to STORAGE_TYPE
                                        and clean them up afterwards
```

# Integration testing for OSN with Minio

We can stand up an integration testing environment using Docker Compose in `/test/s3`:

```sh
docker-compose up -d --build
docker-compose logs -f
```

Test uploads can be generated using `tools/generate-data-dir.py`.

```sh
# generate 100 test uploads
python3 tools/generate-data-dir.py --data-dir test/s3/test-data 100
```

You can open the Minio UI at [http://localhost:9001](http://localhost:9001).

# Integration testing for Pelican

We can stand up an integration testing environment using Docker Compose in `/test/pelican`:

```sh
docker-compose up -d --build
docker-compose logs -f
```

'issuer-key.pem' for use with Pelican can be retrieved here [Pelican Config](https://github.com/waggle-sensor/honeyhouse-config/tree/main/beehives/sage-beehive/config/pelican) or generated using the [SciTokens](https://scitokens.org) Python library and tools. More information can be found here [Pelican Config](https://github.com/waggle-sensor/honeyhouse-config/tree/main/beehives/sage-beehive/config/pelican).

Test uploads can be generated using `tools/generate-data-dir.py`.

```sh
# generate 10 test uploads
python3 tools/generate-data-dir.py --data-dir test/pelican/test-data 10
```

You can check the uploads by curling for them in **LOADER_PELICAN_ENDPOINT**. For example:

```sh
#create jwt token first then run:
curl -v -H "Authorization: Bearer $(cat token)" $(LOADER_PELICAN_ENDPOINT)/$(LOADER_PELICAN_BUCKET)/
```