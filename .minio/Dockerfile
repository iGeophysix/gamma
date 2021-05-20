# Dockerfile
FROM minio/minio

WORKDIR /opt
ADD ./.minio/miniosetup.sh /opt/miniosetup.sh
ADD ./.minio/acl.json /opt/acl.json
RUN curl -O https://dl.min.io/client/mc/release/linux-amd64/mc; \
    chmod +x mc; chmod +x miniosetup.sh;

ENTRYPOINT ./miniosetup.sh
