# Dockerfile
FROM minio/minio

WORKDIR /opt
ADD ./.minio/miniosetup.sh /opt/miniosetup.sh
ADD ./.minio/acl.json /opt/acl.json
ADD ./.minio/mc /opt/mc
RUN chmod +x mc; chmod +x miniosetup.sh;

ENTRYPOINT ./miniosetup.sh
