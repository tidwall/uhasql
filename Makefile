CFLAGS = -O3 \
	-DSQLITE_THREADSAFE=0 \
	-DSQLITE_ENABLE_API_ARMOR \
	-DSQLITE_ENABLE_JSON1 \
	-DSQLITE_ENABLE_RTREE \
	-DSQLITE_SOUNDEX \
	-DSQLITE_ENABLE_GEOPOLY \
	-DSQLITE_USE_ALLOCA \
	-DUHAHA_GOODIES

all: sqlite/libsqlite.a
	cd cmd/uhasql && CGO_ENABLED=1 go build -o ../../uhasql main.go

sqlite/libsqlite.a: sqlite/sqlite.o
	ar rcs sqlite/libsqlite.a sqlite/sqlite.o

clean:
	rm -f sqlite/*.o sqlite/*.a uhasql