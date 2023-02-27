// import {ParquetCompression} from './declare'
import * as Util from './util'
import * as zlib from 'zlib'
import {CompressionCodec as Compression} from '../../thrift/parquet_types'
import {buffer} from 'stream/consumers'
// import * as snappyjs from './snappy'

export interface CompressionCodec {
    deflate(value: Buffer): Buffer
}

export function getCodec(compression: Compression): CompressionCodec {
    switch (compression) {
        case Compression.UNCOMPRESSED:
            return uncompressed
        case Compression.GZIP:
            return gzip
        case Compression.BROTLI:
            return brotli
        default:
            throw new Error(`Unsupported compression`)
    }
}
// export const PARQUET_COMPRESSION_METHODS: Record<ParquetCompression, ParquetCompressionKit> = {
//     UNCOMPRESSED: {
//         deflate: deflate_identity,
//     },
//     GZIP: {
//         deflate: deflate_gzip,
//     },
//     // SNAPPY: {
//     //     deflate: deflate_snappy,
//     // },
//     LZO: {
//         deflate: deflate_lzo,
//     },
//     BROTLI: {
//         deflate: deflate_brotli,
//     },
//     LZ4: {
//         deflate: deflate_lz4,
//     },
// }

const uncompressed: CompressionCodec = {
    deflate(value: Buffer) {
        return value
    },
}

const gzip: CompressionCodec = {
    deflate(value: Buffer) {
        return zlib.gzipSync(value)
    },
}

// const snappy: CompressionCodec = {
//     deflate(value: Buffer) {
//         return snappyjs.deflate(value)
//     }
// }

const brotli: CompressionCodec = {
    deflate(value: Buffer) {
        return zlib.brotliCompressSync(value)
    },
}
