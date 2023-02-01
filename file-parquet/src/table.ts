import assert from 'assert'
import {Table as ArrowTable, Builder, DataType, makeBuilder, tableToIPC} from 'apache-arrow'
import {Compression, WriterProperties, WriterPropertiesBuilder, writeParquet} from 'parquet-wasm/node/arrow1'
import {Table as ITable, TableWriter as ITableWriter} from '@subsquid/file-store'

export interface Type<T> {
    arrowDataType: DataType
    prepare(value: T): any
}

export {Compression}

export interface TableOptions {
    compression?: Compression
    dictionary?: boolean
}

export interface ColumnOptions {
    nullable?: boolean
    compression?: Compression
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
        this.options = {compression: Compression.UNCOMPRESSED, dictionary: false, ...options}
    }

    createWriter(): TableWriter<Convert<T>> {
        return new TableWriter(this.columns, this.options)
    }
}

class TableWriter<T extends Record<string, any>> implements ITableWriter<T> {
    private columnBuilders: Record<string, Builder> = {}
    private writeProperties: WriterProperties
    private _size = 0

    constructor(private columns: Column[], options: Required<TableOptions>) {
        let builer = new WriterPropertiesBuilder()
        for (let column of columns) {
            this.columnBuilders[column.name] = makeBuilder({type: column.data.type.arrowDataType})
            builer = builer.setColumnDictionaryEnabled(column.name, column.data.options.dictionary)
            builer = builer.setColumnCompression(column.name, column.data.options.compression)
        }
        builer = builer.setCompression(options.compression)
        builer = builer.setDictionaryEnabled(options.dictionary)
        this.writeProperties = builer.build()
    }

    get size() {
        return this._size
    }

    flush() {
        this._size = 0

        let columnsData: Record<string, any> = {}
        for (let column of this.columns) {
            columnsData[column.name] = this.columnBuilders[column.name].flush()
        }
        let arrowTable = new ArrowTable(columnsData)

        return writeParquet(tableToIPC(arrowTable), this.writeProperties)
    }

    write(record: T): this {
        return this.writeMany([record])
    }

    writeMany(records: T[]): this {
        let newSize = 0
        for (let column of this.columns) {
            let columnBuilder = this.columnBuilders[column.name]
            for (let record of records) {
                let value = record[column.name]
                if (value == null) {
                    assert(column.data.options.nullable, `Null value in non-nullable column "${column.name}"`)
                }
                columnBuilder.append(value == null ? null : column.data.type.prepare(value))
            }
            newSize += columnBuilder.byteLength
        }
        this._size = newSize

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
        options: {compression: Compression.UNCOMPRESSED, dictionary: false, nullable: false, ...options},
    }
}
