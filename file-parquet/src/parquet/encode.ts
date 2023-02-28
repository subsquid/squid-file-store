import * as codec from '../codec'
import * as compression from '../compression'
import {ParquetColumnChunkData, ParquetDataPageData, RowGroupData} from './declare'
import * as util from './util'
import Int64 from 'node-int64'
import {
    Encoding,
    PageHeader,
    PageType,
    DataPageHeaderV2,
    CompressionCodec,
    RowGroup,
    ColumnChunk,
    ColumnMetaData,
    SchemaElement,
    FileMetaData,
    FieldRepetitionType,
    Type,
} from '../../thrift/parquet_types'
import type {Column} from '../table'
import assert from 'assert'

const PARQUET_VERSION = 2.0

export function encodeDataPage(column: Column, data: ParquetDataPageData): Buffer {
    assert(column.children == null && !column.type.isNested, `Trying to encode nested column`)

    let values = codec.getCodec(Encoding[column.encoding]).encode(column.type.primitiveType, data.values)
    let valuesCompressed = compression.getCodec(CompressionCodec[column.compression]).deflate(values)

    let rLevelsBuf = Buffer.alloc(0)
    if (column.rLevelMax > 0) {
        rLevelsBuf = codec.rle.encode(Type.INT32, data.rLevels, {
            bitWidth: util.getBitWidth(column.rLevelMax),
            disableEnvelope: true,
        })
    }

    let dLevelsBuf = Buffer.alloc(0)
    if (column.dLevelMax > 0) {
        dLevelsBuf = codec.rle.encode(Type.INT32, data.dLevels, {
            bitWidth: util.getBitWidth(column.dLevelMax),
            disableEnvelope: true,
        })
    }

    let header = new PageHeader({
        type: PageType.DATA_PAGE_V2,
        data_page_header_v2: new DataPageHeaderV2({
            definition_levels_byte_length: dLevelsBuf.length,
            repetition_levels_byte_length: rLevelsBuf.length,
            encoding: Encoding[column.encoding],
            num_values: data.valueCount,
            num_nulls: data.valueCount - data.values.length,
            num_rows: data.rowCount,
            is_compressed: column.compression !== 'UNCOMPRESSED',
        }),
        uncompressed_page_size: rLevelsBuf.length + dLevelsBuf.length + values.length,
        compressed_page_size: rLevelsBuf.length + dLevelsBuf.length + valuesCompressed.length,
    })

    return Buffer.concat([util.serializeThrift(header), rLevelsBuf, dLevelsBuf, valuesCompressed])
}

/**
 * Encode an array of values into a parquet column chunk
 */
export function encodeColumnChunk(column: Column, data: ParquetColumnChunkData, offset: number) {
    assert(column.children == null && !column.type.isNested, `Trying to encode nested column`)

    let fragments: Buffer[] = []
    let valueCount = 0
    let totalSize = 0
    for (let dataPageData of data) {
        valueCount += dataPageData.valueCount

        let dataPage = encodeDataPage(column, dataPageData)
        totalSize += dataPage.length

        fragments.push(dataPage)
    }

    /* prepare metadata header */
    let metadata = new ColumnMetaData({
        codec: CompressionCodec[column.compression],
        data_page_offset: new Int64(offset),
        encodings: [Encoding.RLE, Encoding[column.encoding]],
        num_values: new Int64(valueCount),
        path_in_schema: column.path,
        total_compressed_size: new Int64(totalSize),
        total_uncompressed_size: new Int64(totalSize),
        type: column.type.primitiveType,
    })

    /* concat metadata header and data pages */
    let metadataOffset = offset + totalSize
    let body = Buffer.concat([...fragments, util.serializeThrift(metadata)])

    return {body, metadata, metadataOffset}
}

/**
 * Encode a list of column values into a parquet row group
 */
export function encodeRowGroup(
    columns: Column[],
    data: RowGroupData,
    offset: number
): {
    body: Buffer
    metadata: RowGroup
} {
    let fragments: Buffer[] = []
    let columnChunks: ColumnChunk[] = []
    for (let column of columns) {
        if (column.children) continue

        let {body, metadata, metadataOffset} = encodeColumnChunk(column, data.columnData[column.path.join('.')], offset)

        columnChunks.push(
            new ColumnChunk({
                file_offset: new Int64(metadataOffset),
                meta_data: metadata,
            })
        )

        offset += body.length

        fragments.push(body)
    }

    let body = Buffer.concat(fragments)
    let metadata = new RowGroup({
        num_rows: new Int64(data.rowCount),
        columns: columnChunks,
        total_byte_size: new Int64(body.length),
    })

    return {body, metadata}
}

/**
 * Encode a parquet file metadata footer
 */
export function encodeFooter(columns: Column[], rowCount: number, rowGroups: RowGroup[]): Buffer {
    let schema: SchemaElement[] = []

    schema.push(
        new SchemaElement({
            name: 'root',
            num_children: columns.reduce((count, column) => (column.path.length == 1 ? (count += 1) : count), 0), // FIXME: must be done in more propere way
        })
    )

    for (let column of columns) {
        let schemaElement = new SchemaElement({
            name: column.name,
            repetition_type: FieldRepetitionType[column.repetition],
            logicalType: column.type.logicalType,
            converted_type: column.type.convertedType,
        })

        if (column.children) {
            schemaElement.num_children = column.children.reduce(
                (count, child) => (child.path.length == column.path.length + 1 ? (count += 1) : count), // FIXME: must be done in more propere way
                0
            )
        } else if (!column.type.isNested) {
            schemaElement.type = column.type.primitiveType
        } else {
            throw new Error(`Unexpected case: column must have children or non-nested type`)
        }
        schema.push(schemaElement)
    }

    let metadata = new FileMetaData({
        version: PARQUET_VERSION,
        created_by: 'subsquid',
        schema,
        num_rows: new Int64(rowCount),
        row_groups: rowGroups,
    })

    let metadataEncoded = util.serializeThrift(metadata)
    let footerEncoded = Buffer.alloc(metadataEncoded.length + 4)
    metadataEncoded.copy(footerEncoded)
    footerEncoded.writeUInt32LE(metadataEncoded.length, metadataEncoded.length)
    return footerEncoded
}
