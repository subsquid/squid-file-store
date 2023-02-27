import assert from 'assert'
import {Column, Compression, Encoding, TableSchema} from '../table'
import {ShrededColumn} from './declare'

export function shredSchema(
    schema: TableSchema,
    options: {
        path: string[]
        rLevel: number
        dLevel: number
        compression: Compression
        encoding: Encoding
    }
): Column[] {
    let columns: Column[] = []

    for (let columnName in schema) {
        let data = schema[columnName]
        let path = [...options.path, columnName]
        let type = data.type
        let repetition = data.repetition
        let compression = data.compression || options.compression
        let encoding = data.encoding || options.encoding

        let rLevelMax = repetition === 'REPEATED' ? options.rLevel + 1 : options.rLevel
        let dLevelMax = repetition === 'REQUIRED' ? options.dLevel : options.dLevel + 1

        let column: Column = {
            name: columnName,
            path,
            type: data.type,
            repetition,
            rLevelMax,
            dLevelMax,
            compression,
            encoding,
        }
        columns.push(column)

        if (type.isNested) {
            column.children = shredSchema(type.children, {
                path,
                compression,
                encoding,
                dLevel: dLevelMax,
                rLevel: rLevelMax,
            })
            columns.push(...column.children)
        }
    }
    return columns.sort()
}

// FIXME: needs refactoring
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
 *  {
 *    'col1': {
 *      values: [v1, v2, .. vN],
 *      rLevels: [r1, r2, .. rN],
 *      dLevels: [d1, d2, .. dN],
 *      valueCount: N
 *    },
 *    'nested1.col1: {
 *      ...
 *    },
 *    ...
 *  },
 */
export function shredRecord(
    columns: Column[],
    record: Record<string, any> | undefined,
    overwrites: {
        rLevel?: number
        dLevel?: number
    }
): Record<string, ShrededColumn> {
    let res: Record<string, ShrededColumn> = {}

    for (let column of columns) {
        let columnPathStr = column.path.join('.')
        if (Object.keys(res).some((k) => k.startsWith(columnPathStr))) continue

        let field = record?.[column.name]

        let values: any[] | undefined
        switch (column.repetition) {
            case 'REPEATED':
                assert(Array.isArray(field), `Expected array at column "${columnPathStr}"`)
                values = field.length == 0 ? undefined : field
                break
            case 'OPTIONAL':
                values = field == null ? undefined : [field]
                break
            case 'REQUIRED':
                assert(field != null || record == null, `Missing value at column "${columnPathStr}"`)
                values = field == null ? undefined : [field]
                break
            default:
                throw new Error(`Unexpected repetition type ${column.repetition} at column "${columnPathStr}"`)
        }

        if (column.children) {
            assert(column.type.isNested)
            if (values == null) {
                mergeShrededRecords(
                    shredRecord(column.children, undefined, {
                        rLevel: overwrites.rLevel ?? column.rLevelMax,
                        dLevel: overwrites.dLevel ?? column.dLevelMax - 1,
                    }),
                    res
                )
            } else {
                assert(typeof field === 'object')
                for (let i = 0; i < values.length; i++) {
                    let value = column.type.transform(values[i])
                    mergeShrededRecords(
                        shredRecord(column.children, value, {
                            rLevel:
                                overwrites.rLevel ??
                                (i === 0 && column.repetition === 'REPEATED' ? column.rLevelMax - 1 : undefined),
                        }),
                        res
                    )
                }
            }
        } else {
            if (res[columnPathStr] == null) {
                res[columnPathStr] = {
                    values: [],
                    dLevels: [],
                    rLevels: [],
                    valueCount: 0,
                }
            }

            assert(!column.type.isNested)

            if (values == null) {
                res[columnPathStr].rLevels.push(
                    overwrites.rLevel ?? column.repetition === 'REPEATED' ? column.rLevelMax - 1 : column.rLevelMax
                )
                res[columnPathStr].dLevels.push(overwrites.dLevel ?? column.dLevelMax - 1)
                res[columnPathStr].valueCount += 1
            } else {
                for (let i = 0; i < values.length; i++) {
                    let value = column.type.toPrimitive(values[i])
                    res[columnPathStr].rLevels.push(
                        overwrites.rLevel ??
                            (i === 0 && column.repetition === 'REPEATED' ? column.rLevelMax - 1 : column.rLevelMax)
                    )
                    res[columnPathStr].dLevels.push(column.dLevelMax)
                    res[columnPathStr].values.push(value)
                    res[columnPathStr].valueCount += 1
                }
            }
        }
    }

    return res
}

export function mergeShrededRecords(src: Record<string, ShrededColumn>, dst: Record<string, ShrededColumn>) {
    for (let column in src) {
        if (dst[column] == null) {
            dst[column] = src[column]
        } else {
            dst[column].values.push(...src[column].values)
            dst[column].dLevels.push(...src[column].dLevels)
            dst[column].rLevels.push(...src[column].rLevels)
            dst[column].valueCount += src[column].valueCount
        }
    }
}
