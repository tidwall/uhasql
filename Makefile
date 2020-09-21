CFLAGS = -O3 -DSQLITE_THREADSAFE=0 -DSQLITE_ENABLE_RTREE -DUHAHA_GOODIES

all: sqlite/libsqlite.a
	cd cmd/uhasql && go build -o ../../uhasql main.go

sqlite/libsqlite.a: sqlite/sqlite.o
	ar rcs sqlite/libsqlite.a sqlite/sqlite.o

clean:
	rm -f sqlite/*.o sqlite/*.a uhasql