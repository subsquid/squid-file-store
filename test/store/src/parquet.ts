import {Column, Table, Types} from '@subsquid/file-store-parquet'

export const Transfers = new Table('transfers.parquet', {
    blockNumber: Column(Types.Uint32(), {nullable: false}),
    timestamp: Column(Types.Timestamp()),
    from: Column(Types.String()),
    to: Column(Types.String()),
    amount: Column(Types.Uint64()),
})
