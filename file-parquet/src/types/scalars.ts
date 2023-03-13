import {BigDecimal} from '@subsquid/big-decimal'
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
    DecimalType,
} from '../../thrift/parquet_types'
import {getBitWidth} from '../parquet/util'
import {Type} from '../table'

export function String(): Type<string> {
    return {
        primitiveType: PrimitiveType.BYTE_ARRAY,
        convertedType: ConvertedType.UTF8,
        logicalType: new LogicalType({STRING: new StringType()}),
        toPrimitive(value) {
            return Buffer.from(value, 'utf-8')
        },
    }
}

export function Int8(): Type<number> {
    return {
        primitiveType: PrimitiveType.INT32,
        convertedType: ConvertedType.INT_8,
        logicalType: new LogicalType({INTEGER: new IntType({bitWidth: 8, isSigned: true})}),
        toPrimitive(value) {
            assert(-0x80 <= value && value <= 0x7f, `value ${value} does not fit into Int8`)
            return value
        },
    }
}

export function Int16(): Type<number> {
    return {
        primitiveType: PrimitiveType.INT32,
        convertedType: ConvertedType.INT_16,
        logicalType: new LogicalType({INTEGER: new IntType({bitWidth: 16, isSigned: true})}),
        toPrimitive(value) {
            assert(-0x8000 <= value && value <= 0x7fff, `value ${value} does not fit into Int16`)
            return value
        },
    }
}

export function Int32(): Type<number> {
    return {
        primitiveType: PrimitiveType.INT32,
        convertedType: ConvertedType.INT_32,
        logicalType: new LogicalType({INTEGER: new IntType({bitWidth: 32, isSigned: true})}),
        toPrimitive(value) {
            assert(-0x80000000 <= value && value <= 0x7fffffff, `value ${value} does not fit into Int32`)
            return value
        },
    }
}

export function Int64(): Type<bigint> {
    return {
        primitiveType: PrimitiveType.INT64,
        convertedType: ConvertedType.INT_64,
        logicalType: new LogicalType({INTEGER: new IntType({bitWidth: 64, isSigned: true})}),
        toPrimitive(value) {
            assert(
                -0x8000000000000000n <= value && value <= 0x7fffffffffffffffn,
                `value ${value} does not fit into Int64`
            )
            return value
        },
    }
}

export function Uint8(): Type<number> {
    return {
        primitiveType: PrimitiveType.INT32,
        convertedType: ConvertedType.UINT_8,
        logicalType: new LogicalType({INTEGER: new IntType({bitWidth: 8, isSigned: false})}),
        toPrimitive(value) {
            assert(0 <= value && value <= 0xff, `value ${value} does not fit into UInt8`)
            return value
        },
    }
}

export function Uint16(): Type<number> {
    return {
        primitiveType: PrimitiveType.INT32,
        convertedType: ConvertedType.UINT_16,
        logicalType: new LogicalType({INTEGER: new IntType({bitWidth: 16, isSigned: false})}),
        toPrimitive(value) {
            assert(0 <= value && value <= 0xffff, `value ${value} does not fit into UInt16`)
            return value
        },
    }
}

export function Uint32(): Type<number> {
    return {
        primitiveType: PrimitiveType.INT32,
        convertedType: ConvertedType.UINT_32,
        logicalType: new LogicalType({INTEGER: new IntType({bitWidth: 32, isSigned: false})}),
        toPrimitive(value) {
            assert(0 <= value && value <= 0xffffffff, `value ${value} does not fit into UInt32`)
            return value
        },
    }
}

export function Uint64(): Type<bigint> {
    return {
        primitiveType: PrimitiveType.INT64,
        convertedType: ConvertedType.UINT_64,
        logicalType: new LogicalType({INTEGER: new IntType({bitWidth: 64, isSigned: false})}),
        toPrimitive(value) {
            assert(0 <= value && value <= 0xffffffffffffffffn, `value ${value} does not fit into UInt64`)
            return value
        },
    }
}

export function Float(): Type<number> {
    return {
        primitiveType: PrimitiveType.FLOAT,
        toPrimitive(value) {
            assert(!isNaN(value))
            return value
        },
    }
}

export function Double(): Type<number> {
    return {
        primitiveType: PrimitiveType.DOUBLE,
        toPrimitive(value) {
            assert(!isNaN(value))
            return value
        },
    }
}

export function Boolean(): Type<boolean> {
    return {
        primitiveType: PrimitiveType.BOOLEAN,
        toPrimitive(value) {
            return value
        },
    }
}

export function Timestamp(): Type<Date> {
    return {
        primitiveType: PrimitiveType.INT64,
        convertedType: ConvertedType.TIMESTAMP_MILLIS,
        logicalType: new LogicalType({
            TIMESTAMP: new TimestampType({
                unit: new TimeUnit({MILLIS: new MilliSeconds()}),
                isAdjustedToUTC: true,
            }),
        }),
        toPrimitive(value) {
            return BigInt(value.valueOf())
        },
    }
}

export function Decimal(precision: number, scale = 0): Type<number | bigint | BigDecimal> {
    assert(Number.isSafeInteger(precision) && precision > 0, 'invalid precision')
    assert(Number.isSafeInteger(scale) && scale < precision, 'invalid scale')

    let primitiveType: PrimitiveType
    let typeLength: number | undefined

    if (precision <= 9) {
        primitiveType = PrimitiveType.INT32
        typeLength = 4
    } else if (precision <= 18) {
        primitiveType = PrimitiveType.INT64
        typeLength = 8
    } else {
        primitiveType = PrimitiveType.FIXED_LEN_BYTE_ARRAY
        typeLength = Math.floor(getBitWidth(10 ** precision) / 8) + 1
    }

    return {
        primitiveType,
        convertedType: ConvertedType.DECIMAL,
        logicalType: new LogicalType({
            DECIMAL: new DecimalType({scale, precision}),
        }),
        scale,
        precision,
        typeLength,
        toPrimitive(value) {
            let str = value.toString()
            let e = str.indexOf('.')
            if (e > -1) {
                str = str.replace('.', '')
            }

            let s = str.startsWith('-') ? '-' : '+'
            if (s === '-') {
                str = str.slice(1)
            }

            let ei = str.indexOf('e')
            if (ei > -1) {
                let p = Number(str.slice(ei + 1))
                e = e > -1 ? e + p : p
                str = str.slice(0, ei)
            } else if (e < 0) {
                e = str.length
            }

            assert(e <= precision - scale, `value ${value} does not fit into Decimal(${precision}, ${scale})`)
            let len = e + scale + (e > 0 ? 0 : 1)
            if (str.length > len) {
                str = str.slice(0, len) || '0'
            } else if (str.length < len) {
                str = str.padEnd(len, '0')
            }

            let n = s + str
            switch (this.primitiveType) {
                case PrimitiveType.INT32:
                    return Number(n)
                case PrimitiveType.INT64:
                    return BigInt(n)
                case PrimitiveType.FIXED_LEN_BYTE_ARRAY:
                    assert(this.typeLength != null)
                    let bytes = Buffer.alloc(this.typeLength)
                    let v = BigInt(n)
                    for (let i = 0; i < this.typeLength; i++) {
                        let a = v >> (BigInt(i) * 8n)
                        if (a === 0n) break
                        if (a < 0 && a >= -128n) a = (a + 256n) % 256n
                        a &= 0xffn
                        bytes.writeUInt8(Number(a), this.typeLength - 1 - i)
                    }
                    return bytes
                default:
                    throw new Error(`Unexpected PrimitiveType`)
            }
        },
    }
}
