# Dockerfile
FROM python:3.9
ENV LANG=C.UTF-8 LC_ALL=C.UTF-8 PYTHONUNBUFFERED=1 REDIS_HOST=redis

WORKDIR .
COPY celery-requirements.txt celery-requirements.txt
RUN pip install --no-cache-dir -r celery-requirements.txt
RUN rm celery-requirements.txt

RUN groupadd -g 999 celery && \
    useradd -r -u 999 -g celery celery

COPY . /app
WORKDIR /app

RUN chown celery:celery -R /app
USER celery
