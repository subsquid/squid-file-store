// import {ParquetSchema} from './schema'
// import {ParquetEnvelopeWriter, ParquetWriter} from './writer'
// import {writeFileSync} from 'fs'

import {writeFileSync} from 'fs'
import {Types} from '..'
import {CompressionCodec, ConvertedType, Encoding, FieldRepetitionType, Type} from '../../thrift/parquet_types'
import {TableSchema} from '../table'
import {shredSchema} from './shred'
import {ParquetEnvelopeWriter} from './writer'

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
        type: {
            primitiveType: Type.INT32,
            convertedType: ConvertedType.INT_32,
            toPrimitive: (v: any) => v,
        },
        repetition: 'REQUIRED',
        ...defs,
    },
    links: {
        type: {
            isNested: true,
            transform: (v: any) => v,
            children: {
                backward: {
                    type: {
                        primitiveType: Type.INT32,
                        convertedType: ConvertedType.INT_32,
                        toPrimitive: (v: any) => v,
                    },
                    repetition: 'REPEATED',
                    ...defs,
                },
                forward: {
                    type: {
                        primitiveType: Type.INT32,
                        convertedType: ConvertedType.INT_32,
                        toPrimitive: (v: any) => v,
                    },
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
                                type: {
                                    primitiveType: Type.BYTE_ARRAY,
                                    convertedType: ConvertedType.UTF8,
                                    toPrimitive: (v: any) => v,
                                },
                                repetition: 'REQUIRED',
                                ...defs,
                            },
                            country: {
                                type: {
                                    primitiveType: Type.BYTE_ARRAY,
                                    convertedType: ConvertedType.UTF8,
                                    toPrimitive: (v: any) => v,
                                },
                                repetition: 'OPTIONAL',
                                ...defs,
                            },
                        },
                    },
                    repetition: 'REPEATED',
                    ...defs,
                },
                url: {
                    type: {
                        primitiveType: Type.BYTE_ARRAY,
                        convertedType: ConvertedType.UTF8,
                        toPrimitive: (v: any) => v,
                    },
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
//         repetition: FieldRepetitionType.REQUIRED,
//         encoding: Encoding.PLAIN,
//         compression: CompressionCodec.GZIP,
//     },
//     quantity: {
//         type: Types.Int64(),
//         repetition: FieldRepetitionType.REQUIRED,
//         encoding: Encoding.PLAIN,
//         compression: CompressionCodec.GZIP,
//     },
//     price: {
//         type: Types.DOUBLE(),
//         repetition: FieldRepetitionType.REQUIRED,
//         encoding: Encoding.PLAIN,
//         compression: CompressionCodec.GZIP,
//     },
// }

let w = new ParquetEnvelopeWriter(
    shredSchema(schema, {
        path: [],
        compression: 'UNCOMPRESSED',
        encoding: 'PLAIN',
        dLevel: 0,
        rLevel: 0,
    })
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
