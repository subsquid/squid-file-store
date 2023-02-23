import {ParquetCodecOptions, PARQUET_CODEC} from './codec'
import * as compression from './compression'
import {ParquetCodec, ParquetColumnData, ParquetField, ParquetValueArray, PrimitiveType} from './declare'
import {ParquetSchema} from './schema'
import * as util from './util'
import {ParquetWriteBuffer, shredRecord} from './shred'
import Int64 from 'node-int64'
import {
    Encoding,
    PageHeader,
    PageType,
    DataPageHeaderV2,
    Type,
    CompressionCodec,
    RowGroup,
    ColumnChunk,
    ColumnMetaData,
    SchemaElement,
    FileMetaData,
    FieldRepetitionType,
    ConvertedType,
    LogicalType,
} from '../../thrift/parquet_types'

/**
 * Parquet File Magic String
 */
const PARQUET_MAGIC = 'PAR1'

/**
 * Parquet File Format Version
 */
const PARQUET_VERSION = 1

/**
 * Default Page and Row Group sizes
 */
const PARQUET_DEFAULT_PAGE_SIZE = 8192
const PARQUET_DEFAULT_ROW_GROUP_SIZE = 4096

/**
 * Repetition and Definition Level Encoding
 */
const PARQUET_RDLVL_TYPE = 'INT32'
const PARQUET_RDLVL_ENCODING = 'RLE'

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
export class ParquetWriter {
    public schema: ParquetSchema
    public envelopeWriter: ParquetEnvelopeWriter
    public rowBuffer: ParquetWriteBuffer
    public rowGroupSize: number
    public closed: boolean

    /**
     * Create a new buffered parquet writer for a given envelope writer
     */
    constructor(schema: ParquetSchema, opts?: ParquetWriterOptions) {
        this.schema = schema
        this.envelopeWriter = new ParquetEnvelopeWriter(this.schema, 0)
        this.rowBuffer = {columnData: {}, rowCount: 0}
        this.rowGroupSize = opts?.rowGroupSize || PARQUET_DEFAULT_ROW_GROUP_SIZE
        this.closed = false

        // try {
        // } catch (err) {
        //     envelopeWriter.close()
        //     throw err
        // }
    }

    /**
     * Append a single row to the parquet file. Rows are buffered in memory until
     * rowGroupSize rows are in the buffer or close() is called
     */
    appendRow(row: Record<string, any>): void {
        if (this.closed) {
            throw 'writer was closed'
        }

        shredRecord(this.schema, row, this.rowBuffer)
        // if (this.rowBuffer.rowCount >= this.rowGroupSize) {
        //     await this.envelopeWriter.writeRowGroup(this.rowBuffer)
        //     this.rowBuffer = {rowCount: 0, columnData: {}}
        // }
    }

    /**
     * Finish writing the parquet file and commit the footer to disk. This method
     * MUST be called after you are finished adding rows. You must not call this
     * method twice on the same object or add any rows after the close() method has
     * been called
     */
    close(): Buffer {
        if (this.closed) {
            throw new Error('writer was closed')
        }

        this.closed = true

        let buf: Buffer[] = []

        // if (this.rowBuffer.rowCount > 0) {
        buf.push(this.envelopeWriter.writeHeader())
        buf.push(this.envelopeWriter.writeRowGroup(this.rowBuffer))
        this.rowBuffer = {rowCount: 0, columnData: {}}

        // }

        buf.push(this.envelopeWriter.writeFooter())

        return Buffer.concat(buf)
    }

    /**
     * Set the parquet row group size. This values controls the maximum number
     * of rows that are buffered in memory at any given time as well as the number
     * of rows that are co-located on disk. A higher value is generally better for
     * read-time I/O performance at the tradeoff of write-time memory usage.
     */
    setRowGroupSize(cnt: number): void {
        this.rowGroupSize = cnt
    }

    /**
     * Set the parquet data page size. The data page size controls the maximum
     * number of column values that are written to disk as a consecutive array
     */
    setPageSize(cnt: number): void {
        this.envelopeWriter.setPageSize(cnt)
    }
}

/**
 * Create a parquet file from a schema and a number of row groups. This class
 * performs direct, unbuffered writes to the underlying output stream and is
 * intendend for advanced and internal users; the writeXXX methods must be
 * called in the correct order to produce a valid file.
 */
export class ParquetEnvelopeWriter {
    public schema: ParquetSchema

    public offset: number
    public rowCount: number
    public rowGroups: RowGroup[]
    public pageSize: number

    constructor(schema: ParquetSchema, fileOffset: number, opts?: ParquetWriterOptions) {
        this.schema = schema
        this.offset = fileOffset
        this.rowCount = 0
        this.rowGroups = []
        this.pageSize = opts?.pageSize || PARQUET_DEFAULT_PAGE_SIZE
    }

    writeSection(buf: Buffer): Buffer {
        this.offset += buf.length
        return buf
    }

    /**
     * Encode the parquet file header
     */
    writeHeader(): Buffer {
        return this.writeSection(Buffer.from(PARQUET_MAGIC))
    }

    /**
     * Encode a parquet row group. The records object should be created using the
     * shredRecord method
     */
    writeRowGroup(records: ParquetWriteBuffer): Buffer {
        const rowGroup = encodeRowGroup(this.schema, records, {
            baseOffset: this.offset,
            pageSize: this.pageSize,
        })

        this.rowCount += records.rowCount
        this.rowGroups.push(rowGroup.metadata)
        return this.writeSection(rowGroup.body)
    }

    /**
     * Write the parquet file footer
     */
    writeFooter(): Buffer {
        return this.writeSection(encodeFooter(this.schema, this.rowCount, this.rowGroups))
    }

    /**
     * Set the parquet data page size. The data page size controls the maximum
     * number of column values that are written to disk as a consecutive array
     */
    setPageSize(cnt: number): void {
        this.pageSize = cnt
    }
}

/**
 * Encode a consecutive array of data using one of the parquet encodings
 */
function encodeValues(
    type: PrimitiveType,
    encoding: ParquetCodec,
    values: ParquetValueArray,
    opts: ParquetCodecOptions
) {
    if (!(encoding in PARQUET_CODEC)) {
        throw new Error(`invalid encoding: ${encoding}`)
    }
    return PARQUET_CODEC[encoding].encodeValues(type, values, opts)
}

/**
 * Encode a parquet data page
 */
function encodeDataPage(
    column: ParquetField,
    valueCount: number,
    rowCount: number,
    values: any[],
    rlevels: ParquetValueArray,
    dlevels: ParquetValueArray
): Buffer {
    /* encode values */
    let valuesBuf = encodeValues(column.primitiveType!, column.encoding!, values, {
        typeLength: column.typeLength,
        bitWidth: column.typeLength!,
    })

    let valuesBufCompressed = compression.deflate(column.compression!, valuesBuf)

    /* encode repetition and definition levels */
    let rLevelsBuf = Buffer.alloc(0)
    if (column.rLevelMax > 0) {
        rLevelsBuf = encodeValues(PARQUET_RDLVL_TYPE, PARQUET_RDLVL_ENCODING, rlevels, {
            bitWidth: util.getBitWidth(column.rLevelMax),
            disableEnvelope: true,
        })
    }

    let dLevelsBuf = Buffer.alloc(0)
    if (column.dLevelMax > 0) {
        dLevelsBuf = encodeValues(PARQUET_RDLVL_TYPE, PARQUET_RDLVL_ENCODING, dlevels, {
            bitWidth: util.getBitWidth(column.dLevelMax),
            disableEnvelope: true,
        })
    }

    /* build page header */
    let pageHeader = new PageHeader({
        type: PageType.DATA_PAGE_V2,
        data_page_header_v2: new DataPageHeaderV2({
            definition_levels_byte_length: dLevelsBuf.length,
            repetition_levels_byte_length: rLevelsBuf.length,
            encoding: Encoding[column.encoding!],
            num_values: valueCount,
            num_nulls: valueCount - values.length,
            num_rows: rowCount,
            is_compressed: column.compression !== 'UNCOMPRESSED',
        }),
        uncompressed_page_size: rLevelsBuf.length + dLevelsBuf.length + valuesBuf.length,
        compressed_page_size: rLevelsBuf.length + dLevelsBuf.length + valuesBufCompressed.length,
    })

    /* concat page header, repetition and definition levels and values */
    return Buffer.concat([util.serializeThrift(pageHeader), rLevelsBuf, dLevelsBuf, valuesBufCompressed])
}

/**
 * Encode an array of values into a parquet column chunk
 */
function encodeColumnChunk(
    values: ParquetColumnData,
    opts: {
        column: ParquetField
        baseOffset: number
        pageSize?: number
        encoding?: ParquetCodec
        rowCount: number
    }
): {
    body: Buffer
    metadata: ColumnMetaData
    metadataOffset: number
} {
    let pageBuf = encodeDataPage(
        opts.column,
        values.count,
        opts.rowCount,
        values.values,
        values.rlevels,
        values.dlevels
    )

    /* prepare metadata header */
    let metadata = new ColumnMetaData({
        codec: CompressionCodec[opts.column.compression!],
        data_page_offset: new Int64(opts.baseOffset),
        encodings: [Encoding.RLE, Encoding[opts.column.encoding!]],
        num_values: new Int64(values.count),
        path_in_schema: opts.column.path.split('.'),
        total_compressed_size: new Int64(pageBuf.length),
        total_uncompressed_size: new Int64(pageBuf.length),
        type: Type[opts.column.primitiveType!],
    })

    /* concat metadata header and data pages */
    let metadataOffset = opts.baseOffset + pageBuf.length
    let body = Buffer.concat([pageBuf, util.serializeThrift(metadata)])
    return {body, metadata, metadataOffset}
}

/**
 * Encode a list of column values into a parquet row group
 */
function encodeRowGroup(
    schema: ParquetSchema,
    data: ParquetWriteBuffer,
    opts: ParquetWriterOptions
): {
    body: Buffer
    metadata: RowGroup
} {
    let metadata = new RowGroup()
    metadata.num_rows = new Int64(data.rowCount)
    metadata.columns = []

    let body = Buffer.alloc(0)
    for (let field of schema.fieldList) {
        // if (field.fields) {
        //     continue
        // }

        let cchunkData = encodeColumnChunk(data.columnData[field.path], {
            column: field,
            baseOffset: (opts.baseOffset ?? 0) + body.length,
            pageSize: opts.pageSize,
            encoding: field.encoding,
            rowCount: data.rowCount,
        })

        let cchunk = new ColumnChunk()
        cchunk.file_offset = new Int64(cchunkData.metadataOffset)
        cchunk.meta_data = cchunkData.metadata
        metadata.columns.push(cchunk)
        metadata.total_byte_size = new Int64(cchunkData.body.length)

        body = Buffer.concat([body, cchunkData.body])
    }

    return {body, metadata}
}

/**
 * Encode a parquet file metadata footer
 */
function encodeFooter(schema: ParquetSchema, rowCount: number, rowGroups: RowGroup[]): Buffer {
    let metadata = new FileMetaData()
    metadata.version = PARQUET_VERSION
    metadata.created_by = 'subsquid'
    metadata.num_rows = new Int64(rowCount)
    metadata.row_groups = rowGroups
    metadata.schema = []
    metadata.key_value_metadata = []

    let schemaRoot = new SchemaElement()
    schemaRoot.name = 'root'
    schemaRoot.num_children = Object.keys(schema.fields).length
    metadata.schema.push(schemaRoot)

    for (let field of schema.fieldList) {
        let schemaElem = new SchemaElement({
            name: field.name,
            repetition_type: FieldRepetitionType[field.repetitionType],
        })

        // if (field.fields) {
        //     schemaElem.num_children = field.fieldCount
        // } else {
        schemaElem.type = Type[field.primitiveType!]
        // }

        if (field.originalType) {
            schemaElem.converted_type = ConvertedType[field.originalType]
        }

        schemaElem.type_length = field.typeLength

        metadata.schema.push(schemaElem)
    }

    let metadataEncoded = util.serializeThrift(metadata)
    let footerEncoded = Buffer.alloc(metadataEncoded.length + 8)
    metadataEncoded.copy(footerEncoded)
    footerEncoded.writeUInt32LE(metadataEncoded.length, metadataEncoded.length)
    footerEncoded.write(PARQUET_MAGIC, metadataEncoded.length + 4)
    return footerEncoded
}
