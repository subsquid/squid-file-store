import {Column, Table} from '../table'
import {Int32Type, StringType, Uint64Type} from '../types'

import {WriterPropertiesBuilder} from 'parquet-wasm/node/arrow1'

let builder = new WriterPropertiesBuilder()
builder.setCompression(0)
builder.setDictionaryEnabled(true)

export const table = new Table('transfers', {
    blockNumber: Column(Int32Type()),
    extrinsicHash: Column(StringType(), {nullable: true}),
    from: Column(StringType()),
    to: Column(StringType()),
    amount: Column(Uint64Type()),
})

// export let record: TableRecord<typeof table> = {
//     // blockNumber: 5089937,
//     // extrinsicHash: `0x77709e82369fc279c64aeb44c7ec7b05362a3599f0e27b62f6c5a19e3159b6d1`,
//     // from: `EEsp9Duot6U3F1fgA2MXBHeArtkySqpXWYkcBj7wfjC6rLY`,
//     // to: `Cb2QccEAM38pjwmcHHTTuTukUobTHwhakKH4kBo4k8Vur8o`,
//     // amount: 997433334294n,
//     list: ['a', 'b'],
//     from: null,
// }

// function generateRecord(): TableRecord<typeof table> {
//     return {
//         blockNumber: Math.round(5089937 * Math.random()),
//         extrinsicHash: randomString(Math.round(10 * Math.random())),
//         from: randomString(Math.round(10 * Math.random())),
//         to: randomString(Math.round(10 * Math.random())),
//         amount: BigInt(Math.round(5089937 * Math.random())),
//     }
// }

// console.time('wasm')
// let builder = table.createTableBuilder()
// for (let i = 0; i < 1; i++) builder.append(generateRecord())
// let data = builder.flush()
// console.timeEnd('wasm')

// writeFileSync('./test.parquet', data)
// console.log(makeBuilder({type: new Timestamp(TimeUnit.MILLISECOND)}).append(Date.now()).toVector().toJSON())

function randomString(len: number) {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'

    let result = ''
    for (let i = 0; i < len; i++) {
        result += chars[Math.floor(Math.random() * chars.length)]
    }

    return result
}

let a = new WriterPropertiesBuilder().setCompression(0).setDictionaryEnabled(true)
