import fs = require('fs')
import {TBufferedTransport, TCompactProtocol, TFramedTransport} from 'thrift'
import {Writable} from 'stream'

export interface WriteStreamOptions {
    flags?: string
    encoding?: string
    fd?: number
    mode?: number
    autoClose?: boolean
    start?: number
}

class UFramedTransport extends TFramedTransport {
    public readPos: number = 0
}

/**
 * Helper function that serializes a thrift object into a buffer
 */
export function serializeThrift(obj: any): Buffer {
    const output: Buffer[] = []

    const transport = new TBufferedTransport(obj, (buf) => {
        output.push(buf!)
    })

    const protocol = new TCompactProtocol(transport)
    obj.write(protocol)
    transport.flush()

    return Buffer.concat(output)
}

export function decodeThrift(obj: any, buf: Buffer, offset?: number) {
    if (!offset) {
        // tslint:disable-next-line:no-parameter-reassignment
        offset = 0
    }

    const transport = new UFramedTransport(buf)
    transport.readPos = offset
    const protocol = new TCompactProtocol(transport)
    obj.read(protocol)
    return transport.readPos - offset
}

/**
 * Get the number of bits required to store a given value
 */
export function getBitWidth(val: number): number {
    if (val === 0) {
        return 0
        // tslint:disable-next-line:no-else-after-return
    } else {
        return Math.ceil(Math.log2(val + 1))
    }
}

// Supports MQTT path wildcards
// + all immediate children
// # all descendents
export function fieldIndexOf(arr: string[][], elem: string[]): number {
    for (let j = 0; j < arr.length; j++) {
        if (arr[j].length > elem.length) continue
        let m = true
        for (let i = 0; i < elem.length; i++) {
            if (arr[j][i] === elem[i] || arr[j][i] === '+' || arr[j][i] === '#') continue
            if (i >= arr[j].length && arr[j][arr[j].length - 1] === '#') continue
            m = false
            break
        }
        if (m) return j
    }
    return -1
}
