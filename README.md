# UhaSQL

A fault-tolerant Sqlite service running on [Uhaha](https://github.com/tidwall/uhaha).

## Features

- Fault-tolerant using the Raft Consensus Algorithm
- Small memory footprint
- Persists to disk
- Deterministic TIME() and RANDOM() SQL functions
- Uses the Redis protocol, thus any redis client will work with UhaSQL
