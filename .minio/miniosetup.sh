#!/bin/bash
env | grep MINIO
ls -asl

minio server /data &

./mc config host rm local
./mc config host add --quiet --api s3v4 local http://$MINIO_HOST:$MINIO_PORT $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD
./mc mb --quiet local/internal/
./mc policy set none local/internal
./mc mb --quiet local/public/
./mc policy set public local/public/
./mc admin user add local $MINIO_USER $MINIO_PASSWORD
./mc admin policy add local acl ./acl.json
./mc admin policy set local acl user=$MINIO_USER
trap : TERM INT; sleep infinity & wait