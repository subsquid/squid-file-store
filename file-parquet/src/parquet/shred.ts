import assert from 'assert'
import {ParquetColumnData, ParquetField, ParquetValueArray} from './declare'
import {ParquetSchema} from './schema'
import {toPrimitive} from './types'

export class ParquetShredError extends Error {
    constructor(message: string) {
        super(message)
    }
}

export class MissingRequiredFieldShredError extends Error {
    constructor(public fieldName: string) {
        super(`Missing required field: ${fieldName}`)
    }
}

export class TooManyValuesShredError extends Error {
    constructor(public fieldName: string) {
        super(`Multiple values for non-repeated field: ${fieldName}`)
    }
}

export interface ParquetWriteBuffer {
    rowCount: number
    columnData: Record<string, ParquetColumnData>
}

/**
 * 'Shred' a record into a list of <value, repetition_level, definition_level>
 * tuples per column using the Google Dremel Algorithm..
 *
 * The buffer argument must point to an object into which the shredded record
 * will be returned. You may re-use the buffer for repeated calls to this function
 * to append to an existing buffer, as long as the schema is unchanged.
 *
 * The format in which the shredded records will be stored in the buffer is as
 * follows:
 *
 *   buffer = {
 *     columnData: [
 *       'my_col': {
 *          dLevels: [d1, d2, .. dN],
 *          rLevels: [r1, r2, .. rN],
 *          values: [v1, v2, .. vN],
 *        }, ...
 *      ],
 *      rowCount: X,
 *   }
 */
export function shredRecord(schema: ParquetSchema, record: Record<string, any>, buffer: ParquetWriteBuffer): void {
    /* shred the record, this may raise an exception */
    // var recordShredded: Record<string, ParquetColumnData> = {}
    // for (let field of schema.fieldList) {
    //     recordShredded[field.path] = {
    //         dlevels: [],
    //         rlevels: [],
    //         values: [],
    //         count: 0,
    //     }

    //     if (buffer.columnData[field.path] == null) {
    //         buffer.columnData[field.path] = {
    //             dlevels: [],
    //             rlevels: [],
    //             values: [],
    //             count: 0,
    //         }
    //     }
    // }

    // shredRecordInternal(schema.fields, record, recordShredded, 0, 0)
    for (let fieldName in schema.fields) {
        const field = schema.fields[fieldName]
        const fieldType = field.originalType! || field.primitiveType!

        if (buffer.columnData[field.path] == null) {
            buffer.columnData[field.path] = {
                dlevels: [],
                rlevels: [],
                values: [],
                count: 0,
            }
        }

        // fetch values
        let values = []
        if (record && fieldName in record && record[fieldName] !== undefined && record[fieldName] !== null) {
            if (Array.isArray(record[fieldName])) {
                values = record[fieldName]
            } else {
                values.push(record[fieldName])
            }
        }

        if (field.repetitionType === 'REPEATED') {
            // no check require
        } else {
            assert(values.length <= 1, `Too many values for field: ${field.name}`)
            if (field.repetitionType === 'REQUIRED') {
                assert(values.length === 1, `Missing required field: ${field.name}`)
            }
        }

        // push null
        if (values.length == 0) {
            buffer.columnData[field.path].rlevels.push(0)
            buffer.columnData[field.path].dlevels.push(0)
            buffer.columnData[field.path].count += 1
        } else {
            // push values
            for (let i = 0; i < values.length; ++i) {
                const rlvl_i = i === 0 ? 0 : field.rLevelMax

                buffer.columnData[field.path].rlevels.push(rlvl_i)
                buffer.columnData[field.path].dlevels.push(field.dLevelMax)
                buffer.columnData[field.path].values.push(toPrimitive(fieldType, values[i]))
                buffer.columnData[field.path].count += 1
            }
        }
    }

    buffer.rowCount += 1
    // for (let field of schema.fieldList) {
    //     buffer.columnData[field.path].rlevels.push(...recordShredded[field.path].rlevels)

    //     buffer.columnData[field.path].dlevels.push(...recordShredded[field.path].dlevels)

    //     buffer.columnData[field.path].values.push(...recordShredded[field.path].values)

    //     buffer.columnData[field.path].count += recordShredded[field.path].count
    // }
}

// function shredRecordInternal(
//     fields: Record<string, ParquetField>,
//     record: Record<string, any> | null,
//     data: Record<string, ParquetColumnData>,
//     rlvl: number,
//     dlvl: number
// ) {
//     for (let fieldName in fields) {
//         const field = fields[fieldName]
//         const fieldType = field.originalType! || field.primitiveType!

//         // fetch values
//         let values = []
//         if (record && fieldName in record && record[fieldName] !== undefined && record[fieldName] !== null) {
//             if (record[fieldName].constructor === Array) {
//                 values = record[fieldName]
//             } else {
//                 values.push(record[fieldName])
//             }
//         }

//         // check values
//         if (values.length == 0 && !!record && field.repetitionType === 'REQUIRED') {
//             throw 'missing required field: ' + field.name
//         }

//         if (values.length > 1 && field.repetitionType !== 'REPEATED') {
//             throw 'too many values for field: ' + field.name
//         }

//         // push null
//         if (values.length == 0) {
//             if (field.fields) {
//                 shredRecordInternal(field.fields, null, data, rlvl, dlvl)
//             } else {
//                 data[field.path].rlevels.push(rlvl)
//                 data[field.path].dlevels.push(dlvl)
//                 data[field.path].count += 1
//             }
//             continue
//         }

//         // push values
//         for (let i = 0; i < values.length; ++i) {
//             const rlvl_i = i === 0 ? rlvl : field.rLevelMax

//             if (field.fields) {
//                 shredRecordInternal(field.fields, values[i], data, rlvl_i, field.dLevelMax)
//             } else {
//                 data[field.path].rlevels.push(rlvl_i)
//                 data[field.path].dlevels.push(field.dLevelMax)
//                 data[field.path].values.push(toPrimitive(fieldType, values[i]))
//                 data[field.path].count += 1
//             }
//         }
//     }
// }
