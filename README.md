# UhaSQL

A fault-tolerant Sqlite service running on [Uhaha](https://github.com/tidwall/uhaha).

## Features

- Uses the Redis protocol, thus any redis client will work with UhaSQL
- Fault-tolerant using the Raft Consensus Algorithm
- Persists to disk

