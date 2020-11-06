CFLAGS = -O3 \
	-DSQLITE_ENABLE_JSON1 \
	-DSQLITE_ENABLE_RTREE \
	-DSQLITE_SOUNDEX \
	-DSQLITE_ENABLE_GEOPOLY \
	-DSQLITE_USE_ALLOCA \
	-DUHAHA_GOODIES

all: uhasql-server uhasql-cli

sqlite/libsqlite.a: sqlite/sqlite.o
	ar rcs sqlite/libsqlite.a sqlite/sqlite.o

.PHONY: uhasql-server
uhasql-server: sqlite/libsqlite.a
	scripts/build.sh uhasql-server

.PHONY: uhasql-cli
uhasql-cli: 
	scripts/build.sh uhasql-cli

clean:
	rm -f sqlite/*.o sqlite/*.a uhasql-server uhasql-cli

docker:
	scripts/docker.sh

docker-edge:
	scripts/docker.sh --edge

docker-release:
	scripts/docker.sh --release
