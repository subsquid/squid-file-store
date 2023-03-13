import assert from 'assert'
import Int64 from 'node-int64'
import {
    ColumnChunk,
    ColumnMetaData,
    CompressionCodec,
    DataPageHeaderV2,
    Encoding,
    FieldRepetitionType,
    FileMetaData,
    PageHeader,
    PageType,
    RowGroup,
    SchemaElement,
    Type,
} from '../../thrift/parquet_types'
import * as codec from '../codec'
import * as compression from '../compression'
import type {Column} from '../table'
import {ParquetColumnChunkData, ParquetDataPageData, RowGroupData} from './interfaces'
import * as util from './util'

const PARQUET_VERSION = 2.0

export function encodeDataPage(column: Column, data: ParquetDataPageData): Buffer {
    assert(column.children == null && !column.type.isNested, `Trying to encode nested column`)

    let values = codec.getCodec(Encoding[column.encoding]).encode(column.type.primitiveType, data.values)
    let valuesCompressed = compression.getCodec(CompressionCodec[column.compression]).deflate(values)

    let rLevelsBuf: Buffer
    if (column.rLevelMax > 0) {
        rLevelsBuf = codec.rle.encode(Type.INT32, data.rLevels, {
            bitWidth: util.getBitWidth(column.rLevelMax),
            disableEnvelope: true,
        })
    } else {
        rLevelsBuf = Buffer.alloc(0)
    }

    let dLevelsBuf
    if (column.dLevelMax > 0) {
        dLevelsBuf = codec.rle.encode(Type.INT32, data.dLevels, {
            bitWidth: util.getBitWidth(column.dLevelMax),
            disableEnvelope: true,
        })
    } else {
        dLevelsBuf = Buffer.alloc(0)
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

export function encodeColumnChunk(column: Column, data: ParquetColumnChunkData, offset: number) {
    assert(column.children == null && !column.type.isNested, `Trying to encode nested column`)

    let body = Buffer.alloc(0)
    let valueCount = 0
    for (let dataPageData of data) {
        valueCount += dataPageData.valueCount

        let dataPage = encodeDataPage(column, dataPageData)

        dataPage.write
        body = Buffer.concat([body, dataPage])
    }

    let metadata = new ColumnMetaData({
        codec: CompressionCodec[column.compression],
        data_page_offset: new Int64(offset),
        encodings: [Encoding.RLE, Encoding[column.encoding]],
        num_values: new Int64(valueCount),
        path_in_schema: column.path,
        total_compressed_size: new Int64(body.length),
        total_uncompressed_size: new Int64(body.length),
        type: column.type.primitiveType,
    })

    let columnChunk = new ColumnChunk({
        file_offset: new Int64(offset + body.length),
        meta_data: metadata,
    })

    return {body, columnChunk}
}

export function encodeRowGroup(columns: Column[], data: RowGroupData, offset: number) {
    let body = Buffer.alloc(0)
    let columnChunks: ColumnChunk[] = []
    for (let column of columns) {
        if (column.children) continue

        let {body: chunkBody, columnChunk} = encodeColumnChunk(
            column,
            data.columnData[column.path.join('.')],
            offset + body.length
        )

        columnChunks.push(columnChunk)

        body = Buffer.concat([body, chunkBody])
    }

    let rowGroup = new RowGroup({
        num_rows: new Int64(data.rowCount),
        columns: columnChunks,
        total_byte_size: new Int64(body.length),
    })

    return {body, rowGroup}
}

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
            schemaElement.type_length = column.type.typeLength
            schemaElement.scale = column.type.scale
            schemaElement.precision = column.type.precision
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
