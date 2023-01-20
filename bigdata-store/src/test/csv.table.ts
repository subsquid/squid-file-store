import {Column, DateTimeType, IntegerType, StringType, Table} from '@subsquid/bigdata-csv'

export const CsvTable = new Table('transfers', {
    blockNumber: Column(IntegerType()),
    timestamp: Column(DateTimeType()),
    extrinsicHash: Column(StringType(), {nullable: true}),
    from: Column(StringType()),
    to: Column(StringType()),
    amount: Column(IntegerType()),
})
