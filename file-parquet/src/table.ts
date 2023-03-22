import assert from 'assert'
import {Table as ITable, TableWriter as ITableWriter} from '@subsquid/file-store'
import * as parquet from '../thrift/parquet_types'
import {ParquetDataPageData, RowGroupData} from './parquet/interfaces'
import {encodeFooter, encodeRowGroup} from './parquet/encode'
import {shredRecord, shredSchema} from './parquet/shred'

/**
 * Interface for Parquet column data types.
 */
export type Type<T> = {
    logicalType?: parquet.LogicalType
    convertedType?: parquet.ConvertedType
} & (
    | {
          isNested?: false
          primitiveType: parquet.Type
          typeLength?: number
          scale?: number
          precision?: number
          toPrimitive(value: T): any
      }
    | {
          isNested: true
          fields: TableSchema
          transform(value: T): Record<string, any>
      }
)

export type Compression = Extract<
    keyof typeof parquet.CompressionCodec,
    'UNCOMPRESSED' | 'GZIP' | 'LZO' | 'BROTLI' | 'LZ4'
>
export type Encoding = Extract<keyof typeof parquet.Encoding, 'PLAIN'>
export type Repetition = keyof typeof parquet.FieldRepetitionType

export interface TableOptions {
    /**
     * File-wide default compression algorithm. Per-column settings override it.
     *
     * @default 'UNCOMPRESSED'
     */
    compression?: Compression

    /**
     * Target size for row groups before compression. If it is smaller than
     * pageSize times the number of columns, row groups will be roughly of that
     * size and will consist of exactly one page for each column.
     *
     * Unit - bytes
     *
     * @default 32 * 1024 * 1024
     */
    rowGroupSize?: number

    /**
     * Target size for pages.
     *
     * Unit - bytes
     *
     * @default 8 * 1024
     */
    pageSize?: number
}

export interface ColumnOptions {
    /**
     * Whether the column data is nullable.
     *
     * @default false
     */
    nullable?: boolean

    /**
     * Per-column setting of compression algorithm. Overrides the file-wide
     * default set in table options.
     *
     * @default 'UNCOMPRESSED'
     */
    compression?: Compression

    /**
     * Column data encoding.
     *
     * @see https://parquet.apache.org/docs/file-format/data-pages/encodings/
     *
     * @default 'PLAIN'
     */
    encoding?: Encoding
}

export interface ColumnData<T = any, N = boolean> {
    type: Type<T>
    repetition: N extends true ? 'OPTIONAL' : N extends false ? 'REPEATED' | 'REQUIRED' : Repetition
    compression?: Compression
    encoding?: Encoding
}

export interface TableSchema {
    [column: string]: ColumnData
}

type NullableColumns<T extends Record<string, ColumnData>> = {
    [F in keyof T]: T[F]['repetition'] extends 'OPTIONAL' ? F : never
}[keyof T]

type Convert<T extends Record<string, ColumnData>> = {
    [F in Exclude<keyof T, NullableColumns<T>>]: T[F] extends ColumnData<Type<infer R>> ? R : never
} & {
    [F in Extract<keyof T, NullableColumns<T>>]?: T[F] extends ColumnData<Type<infer R>> ? R | null | undefined : never
}

export interface Column {
    name: string
    path: string[]
    type: Type<any>
    repetition: Repetition
    compression: Compression
    encoding: Encoding
    rLevelMax: number
    dLevelMax: number
    children?: Column[]
}

const PARQUET_MAGIC = 'PAR1'
const DEFAULT_PAGE_SIZE = 8 * 1024
const DEFAULT_ROW_GROUP_SIZE = 32 * 1024 * 1024

/**
 * Table interface implementation for writing Parquet files.
 *
 * @see https://docs.subsquid.io/basics/store/file-store/parquet-table/
 */
export class Table<T extends TableSchema> implements ITable<Convert<T>> {
    private columns: Column[] = []
    private options: Required<TableOptions>

    /**
     * Table interface implementation for writing Parquet files.
     *
     * @see https://docs.subsquid.io/basics/store/file-store/parquet-table/
     *
     * @param name - name of the Parquet file in every dataset partition folder
     * @param schema - a mapping from column names to columns (see example)
     * @param options - table options
     *
     * @example
     * ```
     * import {
     *     Table,
     *     Column,
     *     Types
     * } from '@subsquid/file-store-parquet'
     *
     * let transfersTable = new Table(
     *     'transfers.parquet',
     *     {
     *         from: Column(Types.String()),
     *         to: Column(Types.String()),
     *         value: Column(Types.Uint64())
     *     },
     *     {
     *         compression: 'GZIP',
     *         rowGroupSize: 300000,
     *         pageSize: 1000
     *     }
     * )
     * ```
     */
    constructor(readonly name: string, protected schema: T, options?: TableOptions) {
        this.options = {
            compression: 'UNCOMPRESSED',
            pageSize: DEFAULT_PAGE_SIZE,
            rowGroupSize: DEFAULT_ROW_GROUP_SIZE,
            ...options,
        }
        this.columns = shredSchema(schema, {
            path: [],
            compression: this.options.compression || 'UNCOMPRESSED',
            encoding: 'PLAIN',
            dLevel: 0,
            rLevel: 0,
        })
    }

    createWriter(): TableWriter<Convert<T>> {
        return new TableWriter(this.columns, this.options)
    }
}

export class TableWriter<T extends Record<string, any>> implements ITableWriter<T> {
    public rowGroups: RowGroupData[] = []
    public rowCount = 0

    constructor(private columns: Column[], private options: Required<TableOptions>) {}

    get size() {
        let size = 0
        for (let rg of this.rowGroups) {
            size += rg.size
        }
        return size
    }

    appendRecord(record: Record<string, any>) {
        let rowGroup: RowGroupData
        if (this.rowGroups.length == 0 || last(this.rowGroups).size >= this.options.rowGroupSize) {
            rowGroup = {
                columnData: {},
                rowCount: 0,
                size: 0,
            }
            this.rowGroups.push(rowGroup)
        } else {
            rowGroup = last(this.rowGroups)
        }

        let shrededRecord = shredRecord(this.columns, record, {dLevel: 0, rLevel: 0})
        for (let column of this.columns) {
            if (column.children != null) continue

            let columnPathStr = column.path.join('.')
            let columnChunk = rowGroup.columnData[columnPathStr]
            if (columnChunk == null) {
                columnChunk = rowGroup.columnData[columnPathStr] = []
            }

            let dataPage: ParquetDataPageData
            if (columnChunk.length == 0 || last(columnChunk).size >= this.options.pageSize) {
                dataPage = {
                    dLevels: [],
                    rLevels: [],
                    values: [],
                    valueCount: 0,
                    rowCount: 0,
                    size: 0,
                }
                columnChunk.push(dataPage)
            } else {
                dataPage = last(columnChunk)
            }

            dataPage.values.push(...shrededRecord[columnPathStr].values)
            dataPage.dLevels.push(...shrededRecord[columnPathStr].dLevels)
            dataPage.rLevels.push(...shrededRecord[columnPathStr].rLevels)
            dataPage.valueCount += shrededRecord[columnPathStr].valueCount
            dataPage.size += shrededRecord[columnPathStr].size
            dataPage.rowCount += 1

            rowGroup.size += shrededRecord[columnPathStr].size
        }

        rowGroup.rowCount += 1
        this.rowCount += 1
    }

    flush() {
        let body = Buffer.from(PARQUET_MAGIC)
        let rowGroups: parquet.RowGroup[] = []

        for (let rowGroupData of this.rowGroups) {
            let {body: rowGroupBody, rowGroup} = encodeRowGroup(this.columns, rowGroupData, body.length)
            rowGroups.push(rowGroup)

            body = Buffer.concat([body, rowGroupBody])
        }

        body = Buffer.concat([body, encodeFooter(this.columns, this.rowCount, rowGroups), Buffer.from(PARQUET_MAGIC)])

        this.rowGroups = []

        return body
    }

    write(record: T): this {
        return this.writeMany([record])
    }

    writeMany(records: T[]): this {
        for (let rec of records) {
            this.appendRecord(rec)
        }

        return this
    }
}

function last<T>(arr: T[]): T {
    assert(arr.length > 0)
    return arr[arr.length - 1]
}

/**
 * Factory function for Parquet columns
 *
 * @see https://docs.subsquid.io/basics/store/file-store/parquet-table/#columns
 *
 * @param type - a column data type
 * @param options - column options
 */
export function Column<T extends Type<any>>(type: T): ColumnData<T>
export function Column<T extends Type<any>, O extends ColumnOptions>(
    type: T,
    options?: O
): ColumnData<T, O['nullable'] extends true ? true : false>
export function Column(type: Type<any>, options?: ColumnOptions): ColumnData<any> {
    return {
        type,
        repetition: options?.nullable ? 'OPTIONAL' : 'REQUIRED',
        compression: options?.compression,
        encoding: options?.encoding,
    }
}
