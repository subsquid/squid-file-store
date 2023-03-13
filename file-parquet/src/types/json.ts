import {Type} from '../table'
import {Type as PrimitiveType, ConvertedType, LogicalType, JsonType, BsonType} from '../../thrift/parquet_types'
import {toJSON} from '@subsquid/util-internal-json'
import {BSON as BSON_} from 'bson'

type Document = {
    [k: string]: any
}

export function JSON<T extends Document = any>(): Type<T> {
    return {
        primitiveType: PrimitiveType.BYTE_ARRAY,
        convertedType: ConvertedType.JSON,
        logicalType: new LogicalType({JSON: new JsonType()}),
        toPrimitive(value) {
            return Buffer.from(global.JSON.stringify(toJSON(value)), 'utf8')
        },
    }
}

export function BSON<T extends Document = any>(): Type<T> {
    return {
        primitiveType: PrimitiveType.BYTE_ARRAY,
        convertedType: ConvertedType.BSON,
        logicalType: new LogicalType({BSON: new BsonType()}),
        toPrimitive(value) {
            return BSON_.serialize(value)
        },
    }
}
