#!/bin/bash

mkdir -p ./temp

EXAMPLE_DIR="./temp/node-000048b02d15bc8c/uploads/plugin-image-sampler/0.1.1/1621658141892137216-18cbbd563d01ec2c0678f2c129cb8c9c66bd0a05/"
mkdir -p ${EXAMPLE_DIR}
echo '{"ts":1621658141892137216,"shasum":"18cbbd563d01ec2c0678f2c129cb8c9c66bd0a05","labels":{"filename":"sample.jpg", "info": "important"}}'  > ${EXAMPLE_DIR}/meta
echo "hello world" > ${EXAMPLE_DIR}/data
#dd if=/dev/random of=${EXAMPLE_DIR}/data bs=1M count=100



EXAMPLE_DIR="./temp/node-000048b02d15bc8c/uploads/mynamespace/plugin-image-sampler/0.1.1/1621658141892137216-18cbbd563d01ec2c0678f2c129cb8c9c66bd0a05/"
mkdir -p ${EXAMPLE_DIR}
echo '{"timestamp":1621658141892137216,"shasum":"18cbbd563d01ec2c0678f2c129cb8c9c66bd0a05","labels":{"filename":"sample2.jpg"}}'  > ${EXAMPLE_DIR}/meta
echo "hello world" > ${EXAMPLE_DIR}/data


for i in {0..20} ; do
    #echo ${i}
    EXAMPLE_DIR="./temp/node-000048b02d15bc8c/uploads/mynamespace/plugin-image-samplerX/0.1.${i}/1621658141892137216-18cbbd563d01ec2c0678f2c129cb8c9c66bd0a05/"
    mkdir -p ${EXAMPLE_DIR}
    echo '{"timestamp":1621658141892137216,"shasum":"18cbbd563d01ec2c0678f2c129cb8c9c66bd0a05","labels":{"filename":"file'${i}'.jpg"}}'  > ${EXAMPLE_DIR}/meta
    echo "hello world" > ${EXAMPLE_DIR}/data
done