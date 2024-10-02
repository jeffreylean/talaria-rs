## Talaria-rs

Rewrite talaria (written in GO) to Rust. This is not only a pure rewrite,
but also **rethink** how we can leverage latest big data technology to replace the underlying architecture of original
talaria.

Original talaria using badgerdb to store the hot data, we can replace this
with [Apache Arrow](https://arrow.apache.org/) which is a in-memory columnar data format to store all those hot data in
memory instead.

## TODO

1) [ ] Schema builder - Build a Arrow schema based on configuration during compile time.
2) [ ] Buffer writer - Write the incoming data thru RPC to in-memory buffer in Arrow format.
3) [ ] Queryable buffer