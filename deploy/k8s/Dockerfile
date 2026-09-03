ARG ELASTICSEARCH_VERSION=9.4.0
FROM docker.elastic.co/elasticsearch/elasticsearch:${ELASTICSEARCH_VERSION}

ARG PLUGIN_VERSION=1.0.7
COPY --chown=1000:0 paimon-store/build/distributions/paimon-store-${PLUGIN_VERSION}.zip /tmp/paimon-store.zip

RUN bin/elasticsearch-plugin install --batch file:///tmp/paimon-store.zip \
    && rm -f /tmp/paimon-store.zip
