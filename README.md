moved to https://github.com/subsquid/squid-duckdb-store

# @subsquid/csv-store

This package provides CSV based database access to squid mapping.

## Usage example

```ts
const Transfers = new Table('transfers', {
    blockNumber: types.int,
    timestamp: types.datetime,
    extrinsicHash: Nullable(types.string),
    from: types.string,
    to: types.string,
    amount: types.bigint,
})

const db = new CsvDatabase([Transfers], {
    dest: `./data`,
    chunkSize: 10,
    updateInterval: 100_000,
})

processor.run(db, async (ctx) => {
    let transfersData = getTransfers(ctx)
    ctx.store.write(Transfers, transfersData)
})
```
