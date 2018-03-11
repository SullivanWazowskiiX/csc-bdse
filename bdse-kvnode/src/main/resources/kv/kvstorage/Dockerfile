# https://hub.docker.com/_/java/
FROM postgres:10.2

ENV POSTGRES_USER user
ENV POSTGRES_PASSWORD password
ENV POSTGRES_DB storage
ENV POSTGRES_DB storage
ENV PGDATA /var/lib/postgresql/data/pgdata

ADD content/initdb.sh /docker-entrypoint-initdb.d