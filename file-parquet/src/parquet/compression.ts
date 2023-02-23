import {ParquetCompression} from './declare'
import * as Util from './util'
import * as zlib from 'zlib'
import * as snappyjs from './snappy'

let brotli: any
let lzo: any
let lz4js: any

export interface ParquetCompressionKit {
    deflate: (value: Buffer) => Buffer
}

export const PARQUET_COMPRESSION_METHODS: Record<ParquetCompression, ParquetCompressionKit> = {
    UNCOMPRESSED: {
        deflate: deflate_identity,
    },
    GZIP: {
        deflate: deflate_gzip,
    },
    SNAPPY: {
        deflate: deflate_snappy,
    },
    LZO: {
        deflate: deflate_lzo,
    },
    BROTLI: {
        deflate: deflate_brotli,
    },
    LZ4: {
        deflate: deflate_lz4,
    },
}

/**
 * Deflate a value using compression method `method`
 */
export function deflate(method: ParquetCompression, value: Buffer): Buffer {
    if (!(method in PARQUET_COMPRESSION_METHODS)) {
        throw new Error('invalid compression method: ' + method)
    }

    return PARQUET_COMPRESSION_METHODS[method].deflate(value)
}

function deflate_identity(value: Buffer): Buffer {
    return value
}

function deflate_gzip(value: Buffer): Buffer {
    return zlib.gzipSync(value)
}

function deflate_snappy(value: Buffer): Buffer {
    return snappyjs.compress(value)
}

function deflate_lzo(value: Buffer): Buffer {
    return require('lzo').compress(value)
}

function deflate_brotli(value: Buffer): Buffer {
    const result = require('brotli').compress(value, {
        mode: 0,
        quality: 8,
        lgwin: 22,
    })
    return result ? Buffer.from(result) : Buffer.alloc(0)
}

function deflate_lz4(value: Buffer): Buffer {
    try {
        return Buffer.from(require('lz4js').compress(value))
    } catch (err) {
        throw err
    }
}
