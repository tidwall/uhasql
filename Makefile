CFLAGS = -O3 \
	-DSQLITE_ENABLE_JSON1 \
	-DSQLITE_ENABLE_RTREE \
	-DSQLITE_SOUNDEX \
	-DSQLITE_ENABLE_GEOPOLY \
	-DSQLITE_USE_ALLOCA \
	-DUHAHA_GOODIES


all: sqlite/libsqlite.a
	cd cmd/uhasql && \
	CGO_ENABLED=1 go build -ldflags " \
	-X main.buildVersion=$(shell git describe --tags --abbrev=0 2>/dev/null || echo v0.0.0) \
	-X main.buildGitSHA=$(shell git rev-parse --short HEAD 2>/dev/null || echo 0000000)" \
	-o ../../uhasql main.go

sqlite/libsqlite.a: sqlite/sqlite.o
	ar rcs sqlite/libsqlite.a sqlite/sqlite.o

clean:
	rm -f sqlite/*.o sqlite/*.a uhasql