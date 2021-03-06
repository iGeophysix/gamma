# Dockerfile
FROM python:3.9
ENV LANG=C.UTF-8 LC_ALL=C.UTF-8 PYTHONUNBUFFERED=1 REDIS_HOST=redis

WORKDIR .
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
RUN rm requirements.txt

COPY . /app
WORKDIR /app
RUN ls -l