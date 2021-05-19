# Dockerfile
FROM minio/minio

WORKDIR /opt
ADD ./.minio/miniosetup.sh /opt/miniosetup.sh
ADD ./.minio/acl.json /opt/acl.json
RUN curl -O https://drive.google.com/file/d/1-Y-YvniWrMnAwivIp9kjS_P4NsDmoWcv/view?usp=sharing; \
    chmod +x mc; chmod +x miniosetup.sh;

ENTRYPOINT ./miniosetup.sh
