import {Table as ITable, TableWriter as ITableWriter} from '@subsquid/file-store'
import * as parquet from '../thrift/parquet_types'
import {shredSchema} from './parquet/shred'

export type Type<T> = {
    logicalType?: parquet.LogicalType
    convertedType?: parquet.ConvertedType
} & (
    | {
          isNested?: false
          primitiveType: parquet.Type
          toPrimitive: (value: T) => any
      }
    | {
          isNested: true
          children: TableSchema
          transform: (value: T) => Record<string, any>
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

export class Table<T extends TableSchema> implements ITable<Convert<T>> {
    private columns: Column[] = []
    private options: Required<TableOptions>
    constructor(readonly fileName: string, protected schema: T, options?: TableOptions) {
        this.options = {compression: 'UNCOMPRESSED', ...options}
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

class TableWriter<T extends Record<string, any>> implements ITableWriter<T> {
    private writer: ParquetWriter
    private _size = Infinity

    constructor(private columns: Column[], options: Required<TableOptions>) {
        this.writer = new ParquetWriter(
            new ParquetSchema(Object.fromEntries(columns.map((c) => [c.name, {type: c.data.type.parquetType}])))
        )
    }

    get size() {
        return this._size
    }

    flush() {
        return this.writer.close()
    }

    write(record: T): this {
        return this.writeMany([record])
    }

    writeMany(records: T[]): this {
        for (let rec of records) {
            this.writer.appendRow(rec)
        }

        return this
    }
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
