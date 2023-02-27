import {Table as ITable, TableWriter as ITableWriter} from '@subsquid/file-store'
import assert from 'assert'
import * as parquet from '../thrift/parquet_types'
import {RowGroupData, ParquetDataPageData} from './parquet/declare'
import {encodeRowGroup, encodeFooter} from './parquet/encode'
import {shredRecord, shredSchema} from './parquet/shred'

export type Type<T> = {
    logicalType?: parquet.LogicalType
    convertedType?: parquet.ConvertedType
} & (
    | {
          isNested?: false
          primitiveType: parquet.Type
          toPrimitive(value: T): any
          size(value: T): number
      }
    | {
          isNested: true
          children: TableSchema
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
    compression?: Compression
    rowGroupSize?: number
    pageSize?: number
}

export interface ColumnOptions {
    nullable?: boolean
    compression?: Compression
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

export class Table<T extends TableSchema> implements ITable<Convert<T>> {
    private columns: Column[] = []
    private options: Required<TableOptions>
    constructor(readonly name: string, protected schema: T, options?: TableOptions) {
        this.options = {compression: 'UNCOMPRESSED', pageSize: 8192, rowGroupSize: 4096, ...options}
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
        let fragments: Buffer[] = []
        let rowGroups: parquet.RowGroup[] = []

        fragments.push(Buffer.from(PARQUET_MAGIC))

        let offset = PARQUET_MAGIC.length
        for (let rowGroupData of this.rowGroups) {
            let {body, metadata} = encodeRowGroup(this.columns, rowGroupData, offset)
            fragments.push(body)
            rowGroups.push(metadata)

            offset += body.length
        }

        fragments.push(encodeFooter(this.columns, this.rowCount, rowGroups))
        fragments.push(Buffer.from(PARQUET_MAGIC))

        this.rowGroups = []

        return Buffer.concat(fragments)
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

export function Column<T extends Type<any>>(type: T): ColumnData<T>
export function Column<T extends Type<any>, O extends ColumnOptions>(type: T, options?: O): ColumnData<T, true>
export function Column(type: Type<any>, options?: ColumnOptions): ColumnData<any> {
    return {
        type,
        repetition: options?.nullable ? 'OPTIONAL' : 'REQUIRED',
        compression: options?.compression,
        encoding: options?.encoding,
    }
}
