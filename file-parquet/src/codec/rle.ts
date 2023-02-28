import {encode as varintEncode} from 'varint'
import {Type} from '../../thrift/parquet_types'

export interface RLECodecOptions {
    bitWidth: number
    disableEnvelope?: boolean
}

function encodeRunBitpacked(values: number[], opts: RLECodecOptions): Buffer {
    for (let i = 0; i < values.length % 8; i++) {
        values.push(0)
    }

    const buf = Buffer.alloc(Math.ceil(opts.bitWidth * (values.length / 8)))
    for (let b = 0; b < opts.bitWidth * values.length; b++) {
        if ((values[Math.floor(b / opts.bitWidth)] & (1 << b % opts.bitWidth)) > 0) {
            buf[Math.floor(b / 8)] |= 1 << b % 8
        }
    }

    return Buffer.concat([Buffer.from(varintEncode(((values.length / 8) << 1) | 1)), buf])
}

function encodeRunRepeated(value: number, count: number, opts: RLECodecOptions): Buffer {
    const buf = Buffer.alloc(Math.ceil(opts.bitWidth / 8))

    for (let i = 0; i < buf.length; i++) {
        buf.writeUInt8(value & 0xff, i)
        value >> 8
    }

    return Buffer.concat([Buffer.from(varintEncode(count << 1)), buf])
}

export function encode(type: Type, values: any[], opts: RLECodecOptions): Buffer {
    switch (type) {
        case Type.BOOLEAN:
        case Type.INT32:
        case Type.INT64:
            values = values.map((x) => parseInt(x, 10))
            break

        default:
            throw new Error(`unsupported type: ${type}`)
    }

    let buf = Buffer.alloc(0)
    let run = []
    let repeats = 0

    for (let i = 0; i < values.length; i++) {
        // If we are at the beginning of a run and the next value is same we start
        // collecting repeated values
        if (repeats === 0 && run.length % 8 === 0 && values[i] === values[i + 1]) {
            // If we have any data in runs we need to encode them
            if (run.length) {
                buf = Buffer.concat([buf, encodeRunBitpacked(run, opts)])
                run = []
            }
            repeats = 1
        } else if (repeats > 0 && values[i] === values[i - 1]) {
            repeats += 1
        } else {
            // If values changes we need to post any previous repeated values
            if (repeats) {
                buf = Buffer.concat([buf, encodeRunRepeated(values[i - 1], repeats, opts)])
                repeats = 0
            }
            run.push(values[i])
        }
    }

    if (repeats) {
        buf = Buffer.concat([buf, encodeRunRepeated(values[values.length - 1], repeats, opts)])
    } else if (run.length) {
        buf = Buffer.concat([buf, encodeRunBitpacked(run, opts)])
    }

    if (opts.disableEnvelope) {
        return buf
    } else {
        const envelope = Buffer.alloc(buf.length + 4)
        envelope.writeUInt32LE(buf.length)
        buf.copy(envelope, 4)

        return envelope
    }
}
