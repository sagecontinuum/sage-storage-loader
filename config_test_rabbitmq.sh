#!/bin/bash
set -x

RMQ_CONTAINER_NAME=beehive-rabbitmq
ADMIN_USERNAME=test
ADMIN_PASSWORD=test
rmqctl() {
    docker exec ${RMQ_CONTAINER_NAME} rabbitmqctl "$@"
}

rabbitmqadmin() {
    docker exec ${RMQ_CONTAINER_NAME} rabbitmqadmin "$@"
}

create_user(){
    secretname="$1"
    username="$2"
    password="$3"
    confperm="$4"
    writeperm="$5"
    readperm="$6"
    tags="$7"

    while ! rmqctl authenticate_user "$username" "$password"; do
        sleep 1
        while ! (rmqctl add_user "$username" "$password" || rmqctl change_password "$username" "$password"); do
            sleep 3
        done
    done

    while ! rmqctl set_permissions "$username" "$confperm" "$writeperm" "$readperm"; do
        sleep 3
    done

    while ! rmqctl set_user_tags "$username" "$tags"; do
        sleep 3
    done
}


create_user xxx object-store-uploader 'test' '^$' '^waggle.msg$' '^$' 'impersonator'
create_user xxx admin 'admin' '.*' '.*$' '.*' 'administrator'



