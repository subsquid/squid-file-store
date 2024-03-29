import assert from 'assert'
import {BigDecimal} from '@subsquid/big-decimal'
import * as parquet from '../../thrift/parquet_types'
import {getBitWidth} from '../parquet/util'
import {Type} from '../table'

/**
 * @returns the data type for string columns
 */
export function String(): Type<string> {
    let primitiveType: parquet.Type
    let typeLength: number | undefined

    primitiveType = parquet.Type.BYTE_ARRAY

    return {
        primitiveType,
        convertedType: parquet.ConvertedType.UTF8,
        logicalType: new parquet.LogicalType({STRING: new parquet.StringType()}),
        typeLength,
        toPrimitive(value) {
            if (typeLength != null) {
                assert(
                    value.length == typeLength,
                    `invalid string length, expected ${typeLength} but got ${value.length}`
                )
            }
            return Buffer.from(value, 'utf-8')
        },
    }
}

export function Binary(length?: number): Type<Uint8Array> {
    let primitiveType: parquet.Type
    let typeLength: number | undefined
    if (length != null) {
        assert(length > 0 && Number.isSafeInteger(length), `invalid length value`)
        primitiveType = parquet.Type.FIXED_LEN_BYTE_ARRAY
        typeLength = length
    } else {
        primitiveType = parquet.Type.BYTE_ARRAY
    }

    return {
        primitiveType: parquet.Type.BYTE_ARRAY,
        typeLength,
        toPrimitive(value) {
            if (typeLength != null) {
                assert(
                    value.length == typeLength,
                    `invalid byte array length, expected ${typeLength} but got ${value.length}`
                )
            }
            return Buffer.from(value, value.byteOffset)
        },
    }
}

/**
 * @returns the data type for 8-bit signed integer columns
 */
export function Int8(): Type<number> {
    return {
        primitiveType: parquet.Type.INT32,
        convertedType: parquet.ConvertedType.INT_8,
        logicalType: new parquet.LogicalType({INTEGER: new parquet.IntType({bitWidth: 8, isSigned: true})}),
        toPrimitive(value) {
            assert(-0x80 <= value && value <= 0x7f, `value ${value} does not fit into Int8`)
            return value
        },
    }
}

/**
 * @returns the data type for 16-bit signed integer columns
 */
export function Int16(): Type<number> {
    return {
        primitiveType: parquet.Type.INT32,
        convertedType: parquet.ConvertedType.INT_16,
        logicalType: new parquet.LogicalType({INTEGER: new parquet.IntType({bitWidth: 16, isSigned: true})}),
        toPrimitive(value) {
            assert(-0x8000 <= value && value <= 0x7fff, `value ${value} does not fit into Int16`)
            return value
        },
    }
}

/**
 * @returns the data type for 32-bit signed integer columns
 */
export function Int32(): Type<number> {
    return {
        primitiveType: parquet.Type.INT32,
        convertedType: parquet.ConvertedType.INT_32,
        logicalType: new parquet.LogicalType({INTEGER: new parquet.IntType({bitWidth: 32, isSigned: true})}),
        toPrimitive(value) {
            assert(-0x80000000 <= value && value <= 0x7fffffff, `value ${value} does not fit into Int32`)
            return value
        },
    }
}

/**
 * @returns the data type for 64-bit signed integer columns
 */
export function Int64(): Type<bigint | number> {
    return {
        primitiveType: parquet.Type.INT64,
        convertedType: parquet.ConvertedType.INT_64,
        logicalType: new parquet.LogicalType({INTEGER: new parquet.IntType({bitWidth: 64, isSigned: true})}),
        toPrimitive(value) {
            value = typeof value === 'number' ? BigInt(value) : value
            assert(
                -0x8000000000000000n <= value && value <= 0x7fffffffffffffffn,
                `value ${value} does not fit into Int64`
            )
            return value
        },
    }
}

/**
 * @returns the data type for 8-bit unsigned integer columns
 */
export function Uint8(): Type<number> {
    return {
        primitiveType: parquet.Type.INT32,
        convertedType: parquet.ConvertedType.UINT_8,
        logicalType: new parquet.LogicalType({INTEGER: new parquet.IntType({bitWidth: 8, isSigned: false})}),
        toPrimitive(value) {
            assert(0 <= value && value <= 0xff, `value ${value} does not fit into UInt8`)
            return value
        },
    }
}

/**
 * @returns the data type for 16-bit unsigned integer columns
 */
export function Uint16(): Type<number> {
    return {
        primitiveType: parquet.Type.INT32,
        convertedType: parquet.ConvertedType.UINT_16,
        logicalType: new parquet.LogicalType({INTEGER: new parquet.IntType({bitWidth: 16, isSigned: false})}),
        toPrimitive(value) {
            assert(0 <= value && value <= 0xffff, `value ${value} does not fit into UInt16`)
            return value
        },
    }
}

/**
 * @returns the data type for 32-bit unsigned integer columns
 */
export function Uint32(): Type<number> {
    return {
        primitiveType: parquet.Type.INT32,
        convertedType: parquet.ConvertedType.UINT_32,
        logicalType: new parquet.LogicalType({INTEGER: new parquet.IntType({bitWidth: 32, isSigned: false})}),
        toPrimitive(value) {
            assert(0 <= value && value <= 0xffffffff, `value ${value} does not fit into UInt32`)
            return value
        },
    }
}

/**
 * @returns the data type for 64-bit unsigned integer columns
 */
export function Uint64(): Type<bigint | number> {
    return {
        primitiveType: parquet.Type.INT64,
        convertedType: parquet.ConvertedType.UINT_64,
        logicalType: new parquet.LogicalType({INTEGER: new parquet.IntType({bitWidth: 64, isSigned: false})}),
        toPrimitive(value) {
            value = typeof value === 'number' ? BigInt(value) : value
            assert(0 <= value && value <= 0xffffffffffffffffn, `value ${value} does not fit into UInt64`)
            return value
        },
    }
}

/**
 * @returns the data type for 32-bit floating point number columns
 */
export function Float(): Type<number> {
    return {
        primitiveType: parquet.Type.FLOAT,
        toPrimitive(value) {
            assert(!isNaN(value))
            return value
        },
    }
}

/**
 * @returns the data type for 64-bit floating point number columns
 */
export function Double(): Type<number> {
    return {
        primitiveType: parquet.Type.DOUBLE,
        toPrimitive(value) {
            assert(!isNaN(value))
            return value
        },
    }
}

/**
 * @returns the data type for boolean columns
 */
export function Boolean(): Type<boolean> {
    return {
        primitiveType: parquet.Type.BOOLEAN,
        toPrimitive(value) {
            return value
        },
    }
}

/**
 * @returns the data type for UNIX timestamp columns
 */
export function Timestamp(): Type<Date> {
    return {
        primitiveType: parquet.Type.INT64,
        convertedType: parquet.ConvertedType.TIMESTAMP_MILLIS,
        logicalType: new parquet.LogicalType({
            TIMESTAMP: new parquet.TimestampType({
                unit: new parquet.TimeUnit({MILLIS: new parquet.MilliSeconds()}),
                isAdjustedToUTC: true,
            }),
        }),
        toPrimitive(value) {
            return BigInt(value.valueOf())
        },
    }
}

/**
 * @param precision - the number of digits in the number
 *
 * @param scale - the number of digits to the right of the decimal point
 * @default 0
 *
 * @returns the data type for fixed precision decimal number columns
 */
export function Decimal(precision: number, scale = 0): Type<number | bigint | BigDecimal> {
    assert(Number.isSafeInteger(precision) && precision > 0, 'invalid precision value')
    assert(Number.isSafeInteger(scale) && scale < precision, 'invalid scale value')

    let primitiveType: parquet.Type
    let typeLength: number | undefined

    if (precision <= 9) {
        primitiveType = parquet.Type.INT32
    } else if (precision <= 18) {
        primitiveType = parquet.Type.INT64
    } else {
        primitiveType = parquet.Type.FIXED_LEN_BYTE_ARRAY
        typeLength = Math.floor(getBitWidth(10 ** precision) / 8) + 1
    }

    return {
        primitiveType,
        convertedType: parquet.ConvertedType.DECIMAL,
        logicalType: new parquet.LogicalType({
            DECIMAL: new parquet.DecimalType({scale, precision}),
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
                case parquet.Type.INT32:
                    return Number(n)
                case parquet.Type.INT64:
                    return BigInt(n)
                case parquet.Type.FIXED_LEN_BYTE_ARRAY:
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
