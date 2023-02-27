// import {PARQUET_CODEC} from './codec'
// import {PARQUET_COMPRESSION_METHODS} from './compression'
// import {FieldDefinition, ParquetCompression, ParquetField, RepetitionType, SchemaDefinition} from './declare'
// import {PARQUET_LOGICAL_TYPES} from './types'

// /**
//  * A parquet file schema
//  */
// export class ParquetSchema {
//     public schema: Record<string, FieldDefinition>
//     public fields: Record<string, ParquetField>
//     public fieldList: ParquetField[]

//     /**
//      * Create a new schema from a JSON schema definition
//      */
//     constructor(schema: SchemaDefinition) {
//         this.schema = schema
//         this.fields = buildFields(schema, 0, 0, [])
//         this.fieldList = listFields(this.fields)
//     }
// }

// function setCompress(schema: any, type: ParquetCompression) {
//     for (const name in schema) {
//         const node = schema[name]
//         if (node.fields) {
//             setCompress(node.fields, type)
//         } else {
//             node.compression = type
//         }
//     }
// }

// function buildFields(
//     schema: SchemaDefinition,
//     rLevelParentMax: number,
//     dLevelParentMax: number,
//     path: string[]
// ): Record<string, ParquetField> {
//     const fieldList: Record<string, ParquetField> = {}

//     for (const name in schema) {
//         const opts = schema[name]

//         /* field repetition type */
//         const required = !opts.optional
//         const repeated = !!opts.repeated
//         let rLevelMax = rLevelParentMax
//         let dLevelMax = dLevelParentMax

//         let repetitionType: RepetitionType = 'REQUIRED'
//         if (!required) {
//             repetitionType = 'OPTIONAL'
//             ++dLevelMax
//         }

//         if (repeated) {
//             repetitionType = 'REPEATED'
//             ++rLevelMax

//             if (required) {
//                 ++dLevelMax
//             }
//         }

//         const typeDef: any = PARQUET_LOGICAL_TYPES[opts.type]
//         if (!typeDef) {
//             throw new Error(`Invalid parquet type: ${opts.type}`)
//         }

//         opts.encoding = opts.encoding || 'PLAIN'
//         if (!(opts.encoding in PARQUET_CODEC)) {
//             throw new Error(`Unsupported parquet encoding: ${opts.encoding}`)
//         }

//         opts.compression = opts.compression || 'UNCOMPRESSED'
//         if (!(opts.compression in PARQUET_COMPRESSION_METHODS)) {
//             throw new Error(`Unsupported compression method: ${opts.compression}`)
//         }

//         /* add to schema */
//         fieldList[name] = {
//             name: name,
//             primitiveType: typeDef.primitiveType,
//             originalType: typeDef.originalType,
//             path: [...path, name].join('.'),
//             repetitionType: repetitionType,
//             encoding: opts.encoding,
//             compression: opts.compression,
//             typeLength: opts.typeLength || typeDef.typeLength,
//             rLevelMax: rLevelMax,
//             dLevelMax: dLevelMax,
//         }
//     }
//     return fieldList
// }

// function listFields(fields: Record<string, ParquetField>) {
//     let list: ParquetField[] = []

//     for (let k in fields) {
//         let field = fields[k]
//         list.push(field)

//         // if (field.fields) {
//         //     list = list.concat(listFields(field.fields))
//         // }
//     }

//     return list
// }
