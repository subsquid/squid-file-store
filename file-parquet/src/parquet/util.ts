import assert from 'assert'
import {TBufferedTransport, TCompactProtocol} from 'thrift'
import {Type as PrimitiveType} from '../../thrift/parquet_types'

export function serializeThrift(obj: any): Buffer {
    let buffer: Buffer
    const transport = new TBufferedTransport(undefined, (buf) => {
        if (buf) {
            buffer = buf
        }
    })
    const protocol = new TCompactProtocol(transport)
    obj.write(protocol)
    protocol.flush()
    return buffer!
}

export function getBitWidth(val: number): number {
    if (val === 0) {
        return 0
    } else {
        return Math.ceil(Math.log2(val + 1))
    }
}

export function getByteSize(type: PrimitiveType, value?: unknown): number {
    switch (type) {
        case PrimitiveType.INT32:
        case PrimitiveType.FLOAT:
            return 4
        case PrimitiveType.INT64:
        case PrimitiveType.DOUBLE:
            return 8
        case PrimitiveType.INT96:
            return 12
        case PrimitiveType.BOOLEAN:
            return 1
        case PrimitiveType.BYTE_ARRAY:
        case PrimitiveType.FIXED_LEN_BYTE_ARRAY:
            assert(Buffer.isBuffer(value))
            return value.byteLength
    }
}
