import {TBufferedTransport, TCompactProtocol} from 'thrift'

/**
 * Helper function that serializes a thrift object into a buffer
 */
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

/**
 * Get the number of bits required to store a given value
 */
export function getBitWidth(val: number): number {
    if (val === 0) {
        return 0
    } else {
        return Math.ceil(Math.log2(val + 1))
    }
}
