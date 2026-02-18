FROM docker.io/alpine:3.16

LABEL org.opencontainers.image.authors="rschmitz@confluent.io"
LABEL org.opencontainers.image.description="Kafka Streams Container KStreamsFilter based on Alpine Linux"
LABEL org.opencontainers.image.title="KStreamsFilter/Alpine"

RUN apk update \
  && apk upgrade \
  && apk add --update bash openjdk11-jre-headless kcat openssl3 \
  && rm -rf /var/cache/apk/*

COPY maven/lib /app/lib
COPY config /app/config
COPY scripts /app/scripts

RUN chmod 755 /app/scripts/*.sh

WORKDIR /app

CMD ["/app/scripts/run_application.sh", "${project.artifactId}-${project.version}"]
