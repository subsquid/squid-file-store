import {Column, Table, Types} from '@subsquid/file-store-csv'

export const Transfers = new Table('transfers.csv', {
    blockNumber: Column(Types.Integer()),
    timestamp: Column(Types.DateTime()),
    extrinsicHash: Column(Types.String(), {nullable: true}),
    from: Column(Types.String()),
    to: Column(Types.String()),
    amount: Column(Types.Integer()),
})

export const Extrinsics = new Table('extrinsics.csv', {
    blockNumber: Column(Types.Integer()),
    timestamp: Column(Types.DateTime()),
    hash: Column(Types.String()),
    signer: Column(Types.String()),
})
