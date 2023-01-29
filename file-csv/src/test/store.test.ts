import {Database} from '@subsquid/file-store'
import {Table, Column} from '../table'
import {IntegerType, DateTimeType, StringType} from '../types'

const table = new Table('transfers', {
    blockNumber: Column(IntegerType()),
    timestamp: Column(DateTimeType()),
    extrinsicHash: Column(StringType(), {nullable: true}),
    from: Column(StringType()),
    to: Column(StringType()),
    amount: Column(IntegerType()),
})

let db = new Database({
    tables: {
        transfers: table,
    },
    dest: './src/test/data',
    syncIntervalBlocks: 1,
})

db.transact(0, 0, async (store) => {
    // store.transfers.write({to})
})
