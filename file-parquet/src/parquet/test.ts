// import {ParquetSchema} from './schema'
// import {ParquetEnvelopeWriter, ParquetWriter} from './writer'
// import {writeFileSync} from 'fs'

import {writeFileSync} from 'fs'
import {Types} from '..'
import {CompressionCodec, ConvertedType, Encoding, FieldRepetitionType, Type} from '../../thrift/parquet_types'
import {TableSchema, TableWriter} from '../table'
import {shredSchema} from './shred'

// let schema = new ParquetSchema({
//     name: {type: 'UTF8'},
//     quantity: {type: 'INT64', optional: true},
//     price: {type: 'DOUBLE', repeated: true},
// })

// let res: Buffer[] = []

// let write = new ParquetWriter(schema, new ParquetEnvelopeWriter(schema, 0))

// function test() {
//     write.appendRow({
//         name: 'apple',
//         quantity: 10,
//         price: [23.5,21121],
//     })
//     write.appendRow({
//         name: 'apple',
//         quantity: 10,
//         price: [23.5,9823],
//     })
//     write.appendRow({
//         name: 'apple',
//         quantity: 10,
//         price: [23.5,332],
//     })
//     write.appendRow({
//         name: 'apple',
//         quantity: 10,
//         price: [23.5,3121],
//     })
//     write.appendRow({
//         name: 'apple',
//         quantity: 10,
//         price: [23.5,32112],
//     })
//     write.appendRow({
//         name: 'apple',
//         quantity: 10,
//         price: [23.5,1000],
//     })

//     let table = write.close()
//     writeFileSync('test.parquet', table)
// }

const defs = {
    compression: 'UNCOMPRESSED',
    encoding: 'PLAIN',
} as const

let schema: TableSchema = {
    docId: {
        type: Types.Int32(),
        repetition: 'REQUIRED',
        ...defs,
    },
    links: {
        type: {
            isNested: true,
            transform: (v: any) => v,
            children: {
                backward: {
                    type: Types.Int32(),
                    repetition: 'REPEATED',
                    ...defs,
                },
                forward: {
                    type: Types.Int32(),
                    repetition: 'REPEATED',
                    ...defs,
                },
            },
        },
        repetition: 'OPTIONAL',
        ...defs,
    },
    name: {
        type: {
            isNested: true,
            transform: (v: any) => v,
            children: {
                language: {
                    type: {
                        isNested: true,
                        transform: (v: any) => v,
                        children: {
                            code: {
                                type: Types.String(),
                                repetition: 'REQUIRED',
                                ...defs,
                            },
                            country: {
                                type: Types.String(),
                                repetition: 'OPTIONAL',
                                ...defs,
                            },
                        },
                    },
                    repetition: 'REPEATED',
                    ...defs,
                },
                url: {
                    type: Types.String(),
                    repetition: 'OPTIONAL',
                    ...defs,
                },
            },
        },
        repetition: 'REPEATED',
        ...defs,
    },
}

// console.dir(shredSchema(schema), {depth: 2})

// let wbuf = new ParquetWriteBuffer(schema)
// wbuf.shredRecord({
//     docId: 10,
//     links: {
//         backward: [],
//         forward: [20, 40, 60],
//     },
//     name: [
//         {
//             language: [
//                 {
//                     code: 'en-us',
//                     country: 'us',
//                 },
//                 {
//                     code: 'en',
//                 },
//             ],
//             url: 'http://A',
//         },
//         {
//             language: [],
//             url: 'http://B',
//         },
//         {
//             language: [
//                 {
//                     code: 'en-gb',
//                     country: 'gb',
//                 },
//             ],
//         },
//     ],
// })

// console.dir(wbuf.columnData, {depth: 5})

// let wbuf2 = new ParquetWriteBuffer(schema)
// wbuf2.shredRecord({
//     docId: 20,
//     links: {
//         backward: [10, 30],
//         forward: [80],
//     },
//     name: [
//         {
//             url: 'http://C',
//             language: [],
//         },
//     ],
// })
// console.dir(wbuf2.columnData, {depth: 5})

// let schema: TableSchema = {
//     name: {
//         type: Types.String(),
//         repetition: 'REQUIRED',
//         encoding: 'PLAIN',
//         compression: 'GZIP',
//     },
//     quantity: {
//         type: Types.Int64(),
//         repetition: 'REQUIRED',
//         encoding: 'PLAIN',
//         compression: 'GZIP',
//     },
//     price: {
//         type: Types.DOUBLE(),
//         repetition: 'REQUIRED',
//         encoding: 'PLAIN',
//         compression: 'GZIP',
//     },
// }

let w = new TableWriter(
    shredSchema(schema, {
        path: [],
        compression: 'UNCOMPRESSED',
        encoding: 'PLAIN',
        dLevel: 0,
        rLevel: 0,
    }), {
        compression: 'UNCOMPRESSED',
        pageSize: 10000,
        rowGroupSize: 10000,
    }
)

w.appendRecord({
    docId: 10,
    links: {
        backward: [],
        forward: [20, 40, 60],
    },
    name: [
        {
            language: [
                {
                    code: 'en-us',
                    country: 'us',
                },
                {
                    code: 'en',
                },
            ],
            url: 'http://A',
        },
        {
            language: [],
            url: 'http://B',
        },
        {
            language: [
                {
                    code: 'en-gb',
                    country: 'gb',
                },
            ],
        },
    ],
})
// w.appendRecord({
//         docId: 20,
//         links: {
//             backward: [10, 30],
//             forward: [80],
//         },
//         name: [
//             {
//                 url: 'http://C',
//                 language: [],
//             },
//         ],
//     })

// for (let i = 0; i < 1000; i++) {
//     w.appendRecord({
//         name: i.toString(),
//         quantity: 10n,
//         price: 1.0000000000000001,
//     })
// }

writeFileSync('test.parquet', w.flush())
