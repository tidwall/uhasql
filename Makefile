CFLAGS = -O3 -DSQLITE_THREADSAFE=0 -DSQLITE_ENABLE_RTREE -DUHAHA_GOODIES

all: src/sqlite/libsqlite.a
	cd src && go build -o ../uhasql uhasql.go

src/sqlite/libsqlite.a: src/sqlite/sqlite.o
	ar rcs src/sqlite/libsqlite.a src/sqlite/sqlite.o

clean:
	rm -f src/sqlite/*.o src/sqlite/*.a uhasql