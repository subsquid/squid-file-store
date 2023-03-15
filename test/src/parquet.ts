import {Column, Table, Types} from '@subsquid/file-store-parquet'

export const Transfers = new Table('transfers.parquet', {
    blockNumber: Column(Types.Uint32()),
    timestamp: Column(Types.Timestamp()),
    extrinsicHash: Column(Types.String(), {nullable: true}),
    from: Column(Types.String()),
    to: Column(Types.String()),
    amount: Column(Types.Uint64()),
})

export const Extrinsics = new Table('extrinsics.parquet', {
    blockNumber: Column(Types.Uint32()),
    timestamp: Column(Types.Decimal()),
    hash: Column(Types.String()),
    signer: Column(Types.String()),
})
