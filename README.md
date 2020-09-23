# UhaSQL

A fault-tolerant Sqlite service running on [Uhaha](https://github.com/tidwall/uhaha).

## Features

- Fault-tolerant using the Raft Consensus Algorithm
- Small memory footprint
- Persists to disk
- Deterministic TIME() and RANDOM() SQL functions
- Uses the Redis protocol, thus any redis client will work with UhaSQL

## Building

Requires a C compiler and Go 1.15+.

```
make
```

## Running

To start a single node instance. For a full Raft cluster see the [Uhaha README](https://github.com/tidwall/uhaha).

```
./uhasql
```

## Connecting 

Use any Redis client. In this case we'll use the standard `redis-cli`.

```
$ redis-cli -p 11001
```

## Statements

Most any Sqlite statements will work. All statements will return one or more
resultsets, depending on the number of statements that you send to the server
in a single request.

```
> "create table org (name text, department text)"
1) 1) (empty array)
```

Above the "create table" statement did not return any results so we got the
```(empty array)``` resultset. 

Let's insert two records.

```
> "insert into org values ('Janet', 'IT')"
1) 1) (empty array)
> "insert into org values ('Tom', 'Accounting')"
1) 1) (empty array)
```

Ok. Now let's get the do a `select` statement on the table.

```
> "select * from org"
1) 1) 1) "name"
      2) "department"
   2) 1) "janet"
      2) "it"
   3) 1) "tom"
      2) "accounting"
```

This returns a single resultset, which is a series or rows, with the first row
being the column name and the other rows being the values.

## Transactions and multi-statement

In UhaSQL a transaction is just a bunch of statements that are sent as one
request, and each statement seperated by a semicolons. The return value is
either a single error or multiple resultsets.

For example:

```
> "insert into org values ('Andy', 'IT'); select last_insert_rowid();"
1) 1) (empty array)
2) 1) 1) "last_insert_rowid()"
   2) 1) "3"
```

This returned two resultsets. The first is the result to the `insert` statement.
The second is the result to the `select` statement.

You can optionally use the simple `begin` and `end` statements. They really
don't add any nothing but additional clarity, and two extra return values.
```

> "begin; insert into org values ('Monique', 'Executive'); select last_insert_rowid(); end;"
1) 1) (empty array)
2) 1) (empty array)
3) 1) 1) "last_insert_rowid()"
   2) 1) "4"
4) 1) (empty array)
```

## Pitfalls

- `select` statements will run in readonly mode, which do not persist to the
Raft log, thus are very fast. Other commands must persist to the log, including multi-statement that use a mix of `select` and other kinds. Just take care to
avoid mixing big queries with updates. No biggie otherwise.

- All resultsets are buffered on the server prior to sending to the client. So
if you have huge resultsets, like `select * from log` where `log` has a
bazillion records then all bazillion records will be sent back. This will
probably be bad news for your application, which might run out of memory, your
network provider, which might call you with a WTF, and your boss, who will
wonder why the website is slow today. Just make use of the `LIMIT` keyword.
 
Ok, have fun now. Byeeeee!

## License

UhaSQL source code is available under the MIT License.
