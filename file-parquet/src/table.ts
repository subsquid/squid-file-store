import assert from 'assert'
import {Table as ITable, TableWriter as ITableWriter} from '@subsquid/file-store'
import {ParquetCompression, ParquetSchema, ParquetType, ParquetWriter} from './parquet'

export {ParquetCompression}

export interface Type<T> {
    parquetType: ParquetType
}

export interface TableOptions {
    compression?: ParquetCompression
    dictionary?: boolean
}

export interface ColumnOptions {
    nullable?: boolean
    compression?: ParquetCompression
    dictionary?: boolean
}

export interface ColumnData<T extends Type<any> = Type<any>, O extends ColumnOptions = ColumnOptions> {
    type: T
    options: Required<O>
}

export interface TableSchema {
    [column: string]: ColumnData
}

type NullableColumns<T extends Record<string, ColumnData>> = {
    [F in keyof T]: T[F]['options'] extends {nullable: true} ? F : never
}[keyof T]

type Convert<T extends Record<string, ColumnData>> = {
    [F in Exclude<keyof T, NullableColumns<T>>]: T[F] extends ColumnData<Type<infer R>> ? R : never
} & {
    [F in Extract<keyof T, NullableColumns<T>>]?: T[F] extends ColumnData<Type<infer R>> ? R | null | undefined : never
}

export interface Column {
    name: string
    data: ColumnData
}

export class Table<T extends TableSchema> implements ITable<Convert<T>> {
    private columns: Column[] = []
    private options: Required<TableOptions>
    constructor(readonly name: string, protected schema: T, options?: TableOptions) {
        for (let column in schema) {
            this.columns.push({
                name: column,
                data: schema[column],
            })
        }
        this.options = {compression: 'UNCOMPRESSED', dictionary: false, ...options}
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
export function Column<T extends Type<any>, O extends ColumnOptions>(
    type: T,
    options?: O
): ColumnData<T, O & ColumnOptions>
export function Column(type: Type<any>, options?: ColumnOptions): ColumnData {
    return {
        type,
        options: {compression: 'UNCOMPRESSED', dictionary: false, nullable: false, ...options},
    }
}
