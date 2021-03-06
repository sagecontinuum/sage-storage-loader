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

# Integration testing with Minio

We can stand up an integration testing environment using Docker Compose:

```sh
docker-compose up -d --build
docker-compose logs -f
```

Test uploads can be generated using `tools/generate-data-dir.py`.

```sh
# generate 100 test uploads into the default test-data dir
python3 tools/generate-data-dir.py 100
```

You can open the Minio UI at [http://localhost:9001](http://localhost:9001).
