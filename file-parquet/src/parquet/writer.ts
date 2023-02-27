import * as codec from './codec'
import * as compression from './compression'
import {ParquetColumnChunkData, ParquetDataPageData, RowGroupData, ShrededColumn, ShrededRecord} from './declare'
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
import {shredRecord} from './shred'

/**
 * Parquet File Magic String
 */
const PARQUET_MAGIC = 'PAR1'

/**
 * Parquet File Format Version
 */
const PARQUET_VERSION = 2.0

/**
 * Default Page and Row Group sizes
 */
const PARQUET_DEFAULT_PAGE_SIZE = 8192
const PARQUET_DEFAULT_ROW_GROUP_SIZE = 4096

export interface ParquetWriterOptions {
    baseOffset?: number
    rowGroupSize?: number
    pageSize?: number
}

/**
 * Write a parquet file to an output stream. The ParquetWriter will perform
 * buffering/batching for performance, so close() must be called after all rows
 * are written.
 */
// export class ParquetWriter {
//     public schema: ParquetSchema
//     public envelopeWriter: ParquetEnvelopeWriter
//     public rowBuffer: ParquetWriteBuffer
//     public rowGroupSize: number
//     public closed: boolean

//     /**
//      * Create a new buffered parquet writer for a given envelope writer
//      */
//     constructor(schema: ParquetSchema, opts?: ParquetWriterOptions) {
//         this.schema = schema
//         this.envelopeWriter = new ParquetEnvelopeWriter(this.schema, 0)
//         this.rowBuffer = new ParquetWriteBuffer(this.schema)
//         this.rowGroupSize = opts?.rowGroupSize || PARQUET_DEFAULT_ROW_GROUP_SIZE
//         this.closed = false
//     }

//     /**
//      * Append a single row to the parquet file. Rows are buffered in memory until
//      * rowGroupSize rows are in the buffer or close() is called
//      */
//     appendRow(row: Record<string, any>): void {
//         if (this.closed) {
//             throw 'writer was closed'
//         }

//         shredRecord(this.schema, row, this.rowBuffer)
//         // if (this.rowBuffer.rowCount >= this.rowGroupSize) {
//         //     await this.envelopeWriter.writeRowGroup(this.rowBuffer)
//         //     this.rowBuffer = {rowCount: 0, columnData: {}}
//         // }
//     }

//     /**
//      * Finish writing the parquet file and commit the footer to disk. This method
//      * MUST be called after you are finished adding rows. You must not call this
//      * method twice on the same object or add any rows after the close() method has
//      * been called
//      */
//     close(): Buffer {
//         if (this.closed) {
//             throw new Error('writer was closed')
//         }

//         this.closed = true

//         let buf: Buffer[] = []

//         // if (this.rowBuffer.rowCount > 0) {
//         buf.push(this.envelopeWriter.writeHeader())
//         buf.push(this.envelopeWriter.writeRowGroup(this.rowBuffer))
//         this.rowBuffer = {rowCount: 0, columnData: {}}

//         // }

//         buf.push(this.envelopeWriter.writeFooter())

//         return Buffer.concat(buf)
//     }

//     /**
//      * Set the parquet row group size. This values controls the maximum number
//      * of rows that are buffered in memory at any given time as well as the number
//      * of rows that are co-located on disk. A higher value is generally better for
//      * read-time I/O performance at the tradeoff of write-time memory usage.
//      */
//     setRowGroupSize(cnt: number): void {
//         this.rowGroupSize = cnt
//     }

//     /**
//      * Set the parquet data page size. The data page size controls the maximum
//      * number of column values that are written to disk as a consecutive array
//      */
//     setPageSize(cnt: number): void {
//         this.envelopeWriter.setPageSize(cnt)
//     }
// }

/**
 * Create a parquet file from a schema and a number of row groups. This class
 * performs direct, unbuffered writes to the underlying output stream and is
 * intendend for advanced and internal users; the writeXXX methods must be
 * called in the correct order to produce a valid file.
 */
export class ParquetEnvelopeWriter {
    public pageSize: number

    public rowGroups: RowGroupData[] = []
    public rowCount = 0

    constructor(private columns: Column[], opts?: ParquetWriterOptions) {
        console.dir(columns, {depth: 2})
        this.rowCount = 0
        this.pageSize = opts?.pageSize || PARQUET_DEFAULT_PAGE_SIZE
    }

    appendRecord(record: Record<string, any>) {
        // let rowGroup: RowGroupData
        // if (this.rowGroups.length > 0) {
        //     rowGroup = last(this.rowGroups)
        // } else {
        //     rowGroup = {
        //         columnData: {},
        //         rowCount: 0,
        //     }
        //     this.rowGroups.push(rowGroup)
        // }

        // let columnChunk = rowGroup.columnData[columnPathStr]
        // if (columnChunk == null) {
        //     columnChunk = rowGroup.columnData[columnPathStr] = []
        // }

        // let dataPage: ParquetDataPageData
        // if (columnChunk.length > 0) {
        //     dataPage = last(columnChunk)
        // } else {
        //     dataPage = {
        //         dLevels: [],
        //         rLevels: [],
        //         values: [],
        //         valueCount: 0,
        //         rowCount: 0,
        //     }
        //     columnChunk.push(dataPage)
        // }

        let a = shredRecord(this.columns, record, {})
        console.dir(a, {depth: 10})
        // let rowGroup = last(this.rowGroups)
        // rowGroup.rowCount += 1
        // this.rowCount += 1
    }

    flush() {
        let fragments: Buffer[] = []
        let rowGroups: RowGroup[] = []

        fragments.push(Buffer.from(PARQUET_MAGIC))

        let offset = PARQUET_MAGIC.length
        for (let rowGroupData of this.rowGroups) {
            let {body, metadata} = encodeRowGroup(this.columns, rowGroupData, offset)
            fragments.push(body)
            rowGroups.push(metadata)

            offset += body.length
        }

        fragments.push(encodeFooter(this.columns, this.rowCount, rowGroups))

        return Buffer.concat(fragments)
    }
}

/**
 * Encode a parquet data page
 */
function encodeDataPage(column: Column, data: ParquetDataPageData): Buffer {
    assert(column.children == null && !column.type.isNested, `Trying to encode nested column`)

    let values = codec.plain.encodeValues(column.type.primitiveType, data.values, {})
    let valuesCompressed = compression.getCodec(CompressionCodec[column.compression]).deflate(values)

    let rLevelsBuf = Buffer.alloc(0)
    if (column.rLevelMax > 0) {
        rLevelsBuf = codec.rle.encodeValues(Type.INT32, data.rLevels, {
            bitWidth: util.getBitWidth(column.rLevelMax),
            disableEnvelope: true,
        })
    }

    let dLevelsBuf = Buffer.alloc(0)
    if (column.dLevelMax > 0) {
        dLevelsBuf = codec.rle.encodeValues(Type.INT32, data.dLevels, {
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
function encodeColumnChunk(column: Column, data: ParquetColumnChunkData, offset: number) {
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

    console.dir(metadata, {depth: 10})

    /* concat metadata header and data pages */
    let metadataOffset = offset + totalSize
    let body = Buffer.concat([...fragments, util.serializeThrift(metadata)])

    return {body, metadata, metadataOffset}
}

/**
 * Encode a list of column values into a parquet row group
 */
function encodeRowGroup(
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
function encodeFooter(columns: Column[], rowCount: number, rowGroups: RowGroup[]): Buffer {
    let schema: SchemaElement[] = []

    schema.push(
        new SchemaElement({
            name: 'root',
            num_children: columns.length,
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
            schemaElement.num_children = column.children.length
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
    let footerEncoded = Buffer.alloc(metadataEncoded.length + 8)
    metadataEncoded.copy(footerEncoded)
    footerEncoded.writeUInt32LE(metadataEncoded.length, metadataEncoded.length)
    footerEncoded.write(PARQUET_MAGIC, metadataEncoded.length + 4)
    return footerEncoded
}
function last<T>(rowGroups: T[]): T {
    throw new Error('Function not implemented.')
}
