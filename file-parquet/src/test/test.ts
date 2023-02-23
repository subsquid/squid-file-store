// import {Column, Table} from '../table'
// import * as Types from '../types'

// import {WriterPropertiesBuilder} from 'parquet-wasm/node/arrow1'
// import {TableRecord} from '@subsquid/file-store'

// let builder = new WriterPropertiesBuilder()
// builder.setCompression(0)
// builder.setDictionaryEnabled(true)

// export const table = new Table('transfers', {
//     blockNumber: Column(Types.Uint32()),
//     extrinsicHash: Column(Types.String(), {nullable: true}),
//     from: Column(Types.String()),
//     to: Column(Types.String()),
//     amount: Column(Types.Uint64()),
// })

// export let record: TableRecord<typeof table> = {
//     blockNumber: 5089937,
//     extrinsicHash: null,
//     from: `EEsp9Duot6U3F1fgA2MXBHeArtkySqpXWYkcBj7wfjC6rLY`,
//     to: `Cb2QccEAM38pjwmcHHTTuTukUobTHwhakKH4kBo4k8Vur8o`,
//     amount: 997433334294n,
// }

// // function generateRecord(): TableRecord<typeof table> {
// //     return {
// //         blockNumber: Math.round(5089937 * Math.random()),
// //         extrinsicHash: randomString(Math.round(10 * Math.random())),
// //         from: randomString(Math.round(10 * Math.random())),
// //         to: randomString(Math.round(10 * Math.random())),
// //         amount: BigInt(Math.round(5089937 * Math.random())),
// //     }
// // }

// // console.time('wasm')
// // let builder = table.createTableBuilder()
// // for (let i = 0; i < 1; i++) builder.append(generateRecord())
// // let data = builder.flush()
// // console.timeEnd('wasm')

// // writeFileSync('./test.parquet', data)
// // console.log(makeBuilder({type: new Timestamp(TimeUnit.MILLISECOND)}).append(Date.now()).toVector().toJSON())

// function randomString(len: number) {
//     const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'

//     let result = ''
//     for (let i = 0; i < len; i++) {
//         result += chars[Math.floor(Math.random() * chars.length)]
//     }

//     return result
// }

// let a = new WriterPropertiesBuilder().setCompression(0).setDictionaryEnabled(true)
