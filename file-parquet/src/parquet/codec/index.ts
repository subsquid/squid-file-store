import {Type, Encoding} from '../../../thrift/parquet_types'

import * as plain from './plain'

export {plain}
export * as rle from './rle'

export interface Codec {
    encode(type: Type, values: any[]): Buffer
}

export function getCodec(compression: Encoding): Codec {
    switch (compression) {
        case Encoding.PLAIN:
            return plain
        default:
            throw new Error(`Unsupported encoding`)
    }
}
