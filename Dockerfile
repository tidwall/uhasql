# build stage
# Build using Ubuntu/Musl/Go
FROM ubuntu:groovy AS build

RUN apt-get update -y \
    && apt-get upgrade -y \
    && apt-get install -y build-essential wget git musl-tools \
    && wget -q https://golang.org/dl/go1.15.2.linux-amd64.tar.gz \
    && tar -C /usr/local -xzf go1.15.2.linux-amd64.tar.gz \
    && ln -s /usr/local/go/bin/go /usr/local/bin/go \
    && mkdir -p /repo/sqlite

# build the sqlite library
ADD Makefile /repo/Makefile
ADD sqlite/sqlite.c /repo/sqlite/sqlite.c
ADD sqlite/sqlite.h /repo/sqlite/sqlite.h
RUN cd /repo && CC=musl-gcc make sqlite/libsqlite.a

ADD scripts/env.sh /repo/scripts/env.sh
ADD scripts/build.sh /repo/scripts/build.sh
ADD cmd/ /repo/cmd/

# prebuild the app
RUN cd /repo && CC=musl-gcc make

# build the app
ARG GITVERS
ARG GITSHA

RUN cd /repo && GITVERS=$GITVERS GITSHA=$GITSHA CC=musl-gcc make

# run stage
# Run using Alpine
FROM alpine:3.12.0 AS run

RUN apk add --no-cache ca-certificates 

COPY --from=build /repo/uhasql-server /usr/local/bin/uhasql-server
COPY --from=build /repo/uhasql-cli /usr/local/bin/uhasql-cli

RUN chmod +x /usr/local/bin/uhasql-server && \
    chmod +x /usr/local/bin/uhasql-cli

RUN addgroup -S uhasql && \
    adduser -S -G uhasql uhasql && \
    mkdir /data && chown uhasql:uhasql /data

VOLUME /data

EXPOSE 11001
CMD ["uhasql-server", "-d", "/data", "-a", "0.0.0.0:11001"]
