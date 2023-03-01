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
        return require('zlib').gzipSync(value)
    },
}

const snappy: CompressionCodec = {
    deflate(value: Buffer) {
        return require('snappy').compressSync(value)
    },
}

const brotli: CompressionCodec = {
    deflate(value: Buffer) {
        return require('zlib').brotliCompressSync(value)
    },
}

const lz4: CompressionCodec = {
    deflate(value: Buffer) {
        return require('lz4').encode(value)
    },
}

const zstd: CompressionCodec = {
    deflate(value: Buffer) {
        return require('zstd.ts').compressSync({input: value})
    },
}

const lzo: CompressionCodec = {
    deflate(value: Buffer) {
        return require('lzo').compress(value)
    },
}
