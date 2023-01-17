import {Table, StringType, Column, DateTimeType, IntegerType} from '@subsquid/bigdata-csv'

export const Transfers = new Table('transfers', {
    blockNumber: Column(IntegerType()),
    timestamp: Column(DateTimeType()),
    extrinsicHash: Column(StringType(), {nullable: true}),
    from: Column(StringType()),
    to: Column(StringType()),
    amount: Column(IntegerType()),
})

export const Extrinsics = new Table('extrinsics', {
    blockNumber: Column(IntegerType()),
    timestamp: Column(DateTimeType()),
    hash: Column(StringType()),
    signer: Column(StringType()),
})
