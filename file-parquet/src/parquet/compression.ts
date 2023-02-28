import {gzipSync as gzipCompressSync, brotliCompressSync} from 'zlib'
import {encode as lz4CompressSync} from 'lz4'
import {compressSync as snappyCompressSync} from 'snappy'
import {compressSync as zstdCompressSync} from 'zstd.ts'
import {compress as lzoCompressSync} from 'lzo'
import {CompressionCodec as Compression} from '../../thrift/parquet_types'

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
        case Compression.LZ4:
            return lz4
        case Compression.SNAPPY:
            return snappy
        case Compression.ZSTD:
            return zstd
        case Compression.LZO:
            return lzo
        default:
            throw new Error(`Unsupported compression`)
    }
}

const uncompressed: CompressionCodec = {
    deflate(value: Buffer) {
        return value
    },
}

const gzip: CompressionCodec = {
    deflate(value: Buffer) {
        return gzipCompressSync(value)
    },
}

const snappy: CompressionCodec = {
    deflate(value: Buffer) {
        return snappyCompressSync(value)
    },
}

const brotli: CompressionCodec = {
    deflate(value: Buffer) {
        return brotliCompressSync(value)
    },
}

const lz4: CompressionCodec = {
    deflate(value: Buffer) {
        return lz4CompressSync(value)
    },
}

const zstd: CompressionCodec = {
    deflate(value: Buffer) {
        return zstdCompressSync({input: value})
    },
}

const lzo: CompressionCodec = {
    deflate(value: Buffer) {
        return lzoCompressSync(value)
    },
}
