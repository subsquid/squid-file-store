import {TableRecord} from '@subsquid/bigdata-table'
import {DateUnit, Date_, Field, makeBuilder, Struct, Uint16, Uint64, Utf8} from 'apache-arrow'
import {writeFileSync} from 'fs'
import {Column, ParquetTable} from '../table'
import {Int32Type, Int64Type, StringType, UInt64Type} from '../types'

// export const table = new ParquetTable('transfers', {
//     blockNumber: Column(Int32Type()),
//     extrinsicHash: Column(StringType(), {nullable: true}),
//     from: Column(StringType()),
//     to: Column(StringType()),
//     amount: Column(UInt64Type()),
// })

// export let record: TableRecord<typeof table> = {
//     blockNumber: 5089937,
//     extrinsicHash: `0x77709e82369fc279c64aeb44c7ec7b05362a3599f0e27b62f6c5a19e3159b6d1`,
//     from: `EEsp9Duot6U3F1fgA2MXBHeArtkySqpXWYkcBj7wfjC6rLY`,
//     to: `Cb2QccEAM38pjwmcHHTTuTukUobTHwhakKH4kBo4k8Vur8o`,
//     amount: 997433334294n,
// }

// console.time('wasm')
// let builder = table.createTableBuilder()
// for (let i = 0; i < 1_000_000; i++) builder.append(record)
// let data = builder.toTable()
// console.timeEnd('wasm')


console.log(makeBuilder({type: new Struct([Field.new('a', new Uint64())])}).append({a: 5n}).toVector().toArray())