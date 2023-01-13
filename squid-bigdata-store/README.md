# @subsquid/csv-store

This package provides CSV based database access to squid mapping.

## Usage example

```ts
const Transfers = new Table('transfers', {
    blockNumber: types.number,
    timestamp: types.timestamp,
    extrinsicHash: {type: types.string, nullable: true},
    from: types.string,
    to: types.string,
    amount: types.bigint,
})

const db = new CsvDatabase({
    tables: [Transfers],
    dest: `./data`,
    chunkSizeMb: 10,
    syncIntervalBlocks: 1_000,
})

processor.run(db, async (ctx) => {
    let transfersData: TableRecord<typeof Transfers>[] = getTransfers(ctx)
    ctx.store.write(Transfers, transfersData)
})
```
