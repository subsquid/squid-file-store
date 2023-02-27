import assert from 'assert'
import {
    Type as PrimitiveType,
    ConvertedType,
    LogicalType,
    StringType,
    IntType,
    TimestampType,
    TimeUnit,
    MilliSeconds,
} from '../../thrift/parquet_types'
import {Type} from '../table'

export let String = (): Type<string> => ({
    primitiveType: PrimitiveType.BYTE_ARRAY,
    convertedType: ConvertedType.UTF8,
    logicalType: new LogicalType({STRING: new StringType()}),
    toPrimitive(value) {
        return Buffer.from(value, 'utf-8')
    },
})

export let Int8 = (): Type<number> => ({
    primitiveType: PrimitiveType.INT32,
    convertedType: ConvertedType.INT_8,
    logicalType: new LogicalType({INTEGER: new IntType({bitWidth: 8, isSigned: true})}),
    toPrimitive(value) {
        assert(-0x80 <= value && value <= 0x7f)
        return value
    },
})

export let Int16 = (): Type<number> => ({
    primitiveType: PrimitiveType.INT32,
    convertedType: ConvertedType.INT_64,
    logicalType: new LogicalType({INTEGER: new IntType({bitWidth: 16, isSigned: true})}),
    toPrimitive(value) {
        assert(-0x8000 <= value && value <= 0x7fff)
        return value
    },
})

export let Int32 = (): Type<number> => ({
    primitiveType: PrimitiveType.INT32,
    convertedType: ConvertedType.INT_32,
    logicalType: new LogicalType({INTEGER: new IntType({bitWidth: 32, isSigned: true})}),
    toPrimitive(value) {
        assert(-0x80000000 <= value && value <= 0x7fffffff)
        return value
    },
})

export let Int64 = (): Type<bigint> => ({
    primitiveType: PrimitiveType.INT64,
    convertedType: ConvertedType.INT_64,
    logicalType: new LogicalType({INTEGER: new IntType({bitWidth: 64, isSigned: true})}),
    toPrimitive(value) {
        assert(-0x8000000000000000n <= value && value <= 0x7fffffffffffffffn)
        return value
    },
})

export let Uint8 = (): Type<number> => ({
    primitiveType: PrimitiveType.INT32,
    convertedType: ConvertedType.UINT_8,
    logicalType: new LogicalType({INTEGER: new IntType({bitWidth: 8, isSigned: false})}),
    toPrimitive(value) {
        assert(0 <= value && value <= 0xff)
        return value
    },
})

export let Uint16 = (): Type<number> => ({
    primitiveType: PrimitiveType.INT32,
    convertedType: ConvertedType.UINT_16,
    logicalType: new LogicalType({INTEGER: new IntType({bitWidth: 16, isSigned: false})}),
    toPrimitive(value) {
        assert(0 <= value && value <= 0xffff)
        return value
    },
})

export let Uint32 = (): Type<number> => ({
    primitiveType: PrimitiveType.INT32,
    convertedType: ConvertedType.UINT_32,
    logicalType: new LogicalType({INTEGER: new IntType({bitWidth: 32, isSigned: false})}),
    toPrimitive(value) {
        assert(0 <= value && value <= 0xffffffff)
        return value
    },
})

export let Uint64 = (): Type<bigint> => ({
    primitiveType: PrimitiveType.INT64,
    convertedType: ConvertedType.UINT_64,
    logicalType: new LogicalType({INTEGER: new IntType({bitWidth: 64, isSigned: false})}),
    toPrimitive(value) {
        assert(0 <= value && value <= 0xffffffffffffffffn)
        return value
    },
})

export let Float = (): Type<number> => ({
    primitiveType: PrimitiveType.FLOAT,
    toPrimitive(value) {
        assert(!isNaN(value))
        return value
    },
})

export let DOUBLE = (): Type<number> => ({
    primitiveType: PrimitiveType.DOUBLE,
    toPrimitive(value) {
        assert(!isNaN(value))
        return value
    },
})

export let Boolean = (): Type<boolean> => ({
    primitiveType: PrimitiveType.BOOLEAN,
    toPrimitive(value) {
        return value
    },
})

export let Timestamp = (): Type<Date> => ({
    primitiveType: PrimitiveType.INT64,
    convertedType: ConvertedType.TIMESTAMP_MILLIS,
    logicalType: new LogicalType({
        TIMESTAMP: new TimestampType({
            unit: new TimeUnit({MILLIS: new MilliSeconds()}),
            isAdjustedToUTC: true,
        }),
    }),
    toPrimitive(value) {
        return value.valueOf()
    },
})
