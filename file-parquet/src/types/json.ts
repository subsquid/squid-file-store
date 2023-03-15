import {Type} from '../table'
import * as parquet from '../../thrift/parquet_types'
import {toJSON} from '@subsquid/util-internal-json'
import {BSON as BSON_} from 'bson'

type Document = {
    [k: string]: any
}

/**
 * Supply the type of column JSON as the generic parameter.
 *
 * @returns the data type for JSON-valued columns
 *
 * @example
 * ```
 * JSON<{from: string, to: string, value: bigint}>()
 * ```
 */
export function JSON<T extends Document = any>(): Type<T> {
    return {
        primitiveType: parquet.Type.BYTE_ARRAY,
        convertedType: parquet.ConvertedType.JSON,
        logicalType: new parquet.LogicalType({JSON: new parquet.JsonType()}),
        toPrimitive(value) {
            return Buffer.from(global.JSON.stringify(toJSON(value)), 'utf8')
        },
    }
}

/**
 * Supply the type of column BSON as the generic parameter.
 *
 * @see serialize(object) at https://github.com/mongodb/js-bson#functions
 *
 * @returns the data type for BSON-valued columns
 *
 * @example
 * ```
 * BSON<{from: string, to: string}>()
 * ```
 */
export function BSON<T extends Document = any>(): Type<T> {
    return {
        primitiveType: parquet.Type.BYTE_ARRAY,
        convertedType: parquet.ConvertedType.BSON,
        logicalType: new parquet.LogicalType({BSON: new parquet.BsonType()}),
        toPrimitive(value) {
            return BSON_.serialize(value)
        },
    }
}
