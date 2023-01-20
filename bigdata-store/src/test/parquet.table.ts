import {Column, StringType, Table, TimestampType, Uint16Type, Uint64Type} from '@subsquid/bigdata-parquet'

export const ParquetTable = new Table('transfers', {
    blockNumber: Column(Uint16Type()),
    timestamp: Column(TimestampType()),
    extrinsicHash: Column(StringType(), {nullable: true}),
    from: Column(StringType()),
    to: Column(StringType()),
    amount: Column(Uint64Type()),
})
