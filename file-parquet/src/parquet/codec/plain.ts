// import {ParquetValueArray, PrimitiveType} from '../declare'
import {CursorBuffer, ParquetCodecOptions} from './declare'
import int53 from 'int53'
import {Type} from '../../../thrift/parquet_types'
import Int64 from 'node-int64'

const systemIsLittleEndian = new DataView(new Int32Array([1]).buffer).getInt32(0, true) === 1

export function encodeValues(type: Type, values: any[], opts: ParquetCodecOptions): Buffer {
    switch (type) {
        case Type.BOOLEAN:
            return encodeValues_BOOLEAN(values)
        case Type.INT32:
            return encodeValues_INT32(values)
        case Type.INT64:
            return encodeValues_INT64(values)
        case Type.FLOAT:
            return encodeValues_FLOAT(values)
        case Type.DOUBLE:
            return encodeValues_DOUBLE(values)
        case Type.BYTE_ARRAY:
            return encodeValues_BYTE_ARRAY(values)
        case Type.FIXED_LEN_BYTE_ARRAY:
            return encodeValues_FIXED_LEN_BYTE_ARRAY(values as Buffer[], opts)
        default:
            throw new Error(`Unsupported type: ${type}`)
    }
}

/**
 * Encode an array of booleans as a bit sequence.
 *
 * The resulting buffer will be rounded up in size to the nearest whole byte.
 *
 * If the parameter is not actually an array of booleans, "truthy" values will
 * be written with 1, other values will be written as 0.
 */
function encodeValues_BOOLEAN(values: boolean[]): Buffer {
    const buf = Buffer.alloc(Math.ceil(values.length / 8))
    buf.fill(0)
    for (let i = 0; i < values.length; i++) {
        if (values[i]) {
            buf[Math.floor(i / 8)] |= 1 << i % 8
        }
    }
    return buf
}

/**
 * Encode INT32 values to binary.
 *
 * Note that if the input is not an array of number or an Int32Array
 * this may throw an exception.
 */
function encodeValues_INT32(values: number[]): Buffer {
    // On little-endian systems we can use typed array to avoid data copying
    if (systemIsLittleEndian) {
        const tab = Int32Array.from(values)
        return Buffer.from(tab.buffer.slice(tab.byteOffset, tab.byteLength))
    } else {
        const buf = Buffer.alloc(4 * values.length)
        for (let i = 0; i < values.length; i++) {
            buf.writeInt32LE(values[i], i * 4)
        }
        return buf
    }
}

/**
 * Encode INT64 values to a buffer.
 */
function encodeValues_INT64(values: bigint[]): Buffer {
    const buf = Buffer.alloc(8 * values.length)

    if (systemIsLittleEndian) {
        const tab = BigInt64Array.from(values)
        return Buffer.from(tab.buffer.slice(tab.byteOffset, tab.byteLength))
    } else {
        const buf = Buffer.alloc(8 * values.length)
        for (let i = 0; i < values.length; i++) {
            buf.writeBigInt64LE(values[i], i * 8)
        }
        return buf
    }
    return buf
}

/**
 * Encode FLOAT values from an array of numbers or a Float32Array
 */
function encodeValues_FLOAT(values: number[]): Buffer {
    // On little-endian systems we can use typed array
    if (systemIsLittleEndian) {
        const tab = values instanceof Float32Array ? values : Float32Array.from(values as number[])
        return Buffer.from(tab.buffer.slice(tab.byteOffset, tab.byteLength))
    }
    const buf = Buffer.alloc(4 * values.length)
    for (let i = 0; i < values.length; i++) {
        buf.writeFloatLE(values[i] as number, i * 4)
    }
    return buf
}

/**
 * Encode DOUBLE values from an array of numbers or a Float64Array.
 */
function encodeValues_DOUBLE(values: number[]): Buffer {
    // On little-endian systems with 8-byte aligned data we can avoid data copying
    if (systemIsLittleEndian) {
        const tab = Float64Array.from(values)
        return Buffer.from(tab.buffer.slice(tab.byteOffset, tab.byteLength))
    }
    const buf = Buffer.alloc(8 * values.length)
    for (let i = 0; i < values.length; i++) {
        buf.writeDoubleLE(values[i], i * 8)
    }
    return buf
}

function encodeValues_BYTE_ARRAY(values: Buffer[]): Buffer {
    let buf_len = 0
    for (let i = 0; i < values.length; i++) {
        const value = values[i]
        const buf = (values[i] = Buffer.from(value))
        buf_len += 4 + buf.length
    }
    const buf = Buffer.alloc(buf_len)
    let buf_pos = 0
    for (let i = 0; i < values.length; i++) {
        const value = values[i]
        buf.writeUInt32LE(value.length, buf_pos)
        value.copy(buf, buf_pos + 4)
        buf_pos += 4 + value.length
    }
    return buf
}

function encodeValues_FIXED_LEN_BYTE_ARRAY(values: Buffer[], opts: ParquetCodecOptions): Buffer {
    if (!opts.typeLength) {
        throw new Error('missing option: typeLength (required for FIXED_LEN_BYTE_ARRAY)')
    }
    if (!values.every((val) => val.length === opts.typeLength)) {
        throw new Error('not all values for FIXED_LEN_BYTE_ARRAY have the correct length')
    }
    if (values.every((val) => Buffer.isBuffer(val))) {
        return Buffer.concat(values as Buffer[])
    }
    return Buffer.concat(values.map((val) => (Buffer.isBuffer(val) ? val : Buffer.from(val))))
}