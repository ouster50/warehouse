FROM cassandra:4.1

COPY schema.cql /schema.cql

COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
