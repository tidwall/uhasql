# uhasql

A high available Sqlite service running on [Uhaha](https://github.com/tidwall/uhaha).

This README focuses on the features distinct to UhaSQL. There are a bunch of other features that are inherited from the Uhaha such as a Raft cluster, TLS, Auth passwords, Backups, Snapshots, etc. Please see the [Uhaha README](https://github.com/tidwall/uhaha) for more detailed information on these advanced features.

## Features

- High availablity using the Raft Consensus Algorithm
- Small memory footprint
- Persists to disk
- Stored procedure scripts
- Deterministic TIME() and RANDOM() SQL functions
- Uses the Redis protocol, thus any redis client will work with UhaSQL
- Security features like TLS and Auth passwords

## Building

Requires a C compiler and Go 1.15+.

```
make
```

## Running

To start a single node instance. 

```
$ ./uhasql-server
```

## Connecting 

You can use Redis client to work with UhaSQL, but I've included a specialzed
tool `uhasql-cli` for tinkering with the database from the command line.
It works a lot like the `sqlite3` command line tool.

```
$ ./uhasql-cli
```

## Statements

Most any Sqlite statements will work. All statements will return one or more
resultsets, depending on the number of statements that you send to the server
in a single request.

```
uhasql> create table org (name text, department text);
```

Let's insert two records.

```
uhasql> insert into org values ('Janet', 'IT');
uhasql> insert into org values ('Tom', 'Accounting');
```

Ok. Now let's get the do a `select` statement on the table.

```
uhasql> select * from org;
name   department
-----  ----------
Janet  IT
Tom    Accounting
Tom    Accounting
```

This returns a single resultset, which is a series or rows, with the first row
being the column name and the other rows being the values.

## Transactions / multi-statement request

In UhaSQL a transaction is just a bunch of statements that are sent as one
request with each statement seperated by a semicolon. The return value is
either a single error or multiple resultsets.

For example:

```
uhasql> insert into org values ('Andy', 'IT'); select last_insert_rowid();
last_insert_rowid()
-------------------
4
```

## Store procedure scripts

Sqlite does not have support for traditional stored procedures, but uhasql
added the ability to create specialized stored procedure scripts. These procs
are written in Javascript (ecma 5), always run in "write" mode, and
automatically rollback on exceptions.

A proc script uses standard javascript (ecma 5) with the addition of one new
function: `exec(sqlStmt)`, which returns the resultset for the provided 
sql statment. If the `exec` call results in an error then an exception is 
thrown and the script rollsback.

To create a new procedure:

```
PROC SET name script
```

To run the procedure:

```
PROC EXEC name [arg ...]
```

The `uhasql-cli` requires the special `\`\`\`` three backticks for setting a
multiline procedure. For example:

```
uhasql> proc set new_person_if_empty \`\`\`
   ...> var res = exec("select count(*) from org");
   ...> var count = parseInt(res[1][0]);
   ...> if (count == 0) {
   ...>   exec("insert into org (name) values ('Stevie Nicks')");
   ...> }
   ...> \`\`\`
uhasql>
```

Which creates the stored proc:

```js
var res = exec("select count(*) from org");
var count = parseInt(res[1][0]);
if (count == 0) {
  exec("insert into org (name) values ('Stevie Nicks')");
}
```

The other `PROC` operations are:

```
PROC GET name
PROC DEL name
PROC LIST
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
