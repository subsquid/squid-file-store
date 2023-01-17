import assert from 'assert'
import {
    TableSchema,
    Table as BaseTable,
    TableRecord,
    ITableBuilder,
    Column,
    ColumnData,
    ColumnOptions,
} from '@subsquid/bigdata-table'
import {ParquetType} from './types'
import {Builder, makeBuilder, Table as ArrowTable, tableToIPC} from 'apache-arrow'
import {writeParquet, WriterProperties, WriterPropertiesBuilder, Compression} from 'parquet-wasm/node/arrow1'

export {Compression} from 'parquet-wasm/node/arrow1'

export interface TableOptions {
    compression?: Compression
    dictionary?: boolean
}

export interface ParquetColumnOptions extends ColumnOptions {
    compression?: Compression
    dictionary?: boolean
}

type ParquetColumnData<
    T extends ParquetType<any> = ParquetType<any>,
    O extends ParquetColumnOptions = ParquetColumnOptions
> = ColumnData<T, O>

export class Table<T extends TableSchema<ParquetColumnData>> extends BaseTable<T> {
    private options: Required<TableOptions>
    constructor(readonly name: string, protected schema: T, options?: TableOptions) {
        super(name, schema)
        this.options = {compression: Compression.UNCOMPRESSED, dictionary: false, ...options}
    }

    createTableBuilder(): TableBuilder<T> {
        return new TableBuilder(this.columns, this.options)
    }

    getFileExtension() {
        return 'parquet'
    }
}

class TableBuilder<T extends TableSchema<ParquetColumnData>> implements ITableBuilder<T> {
    private columnBuilders: Record<string, Builder> = {}
    private writeProperties: WriterProperties
    private _size = 0

    constructor(private columns: Column<ParquetColumnData>[], private options: Required<TableOptions>) {
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

    append(records: TableRecord<T> | TableRecord<T>[]): TableBuilder<T> {
        records = Array.isArray(records) ? records : [records]

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

export function Column<T extends ParquetType<any>>(type: T): ParquetColumnData<T>
export function Column<T extends ParquetType<any>, O extends ParquetColumnOptions>(
    type: T,
    options?: O
): ParquetColumnData<T, O & ParquetColumnOptions>
export function Column<T extends ParquetType<any>>(type: T, options?: ParquetColumnOptions) {
    return {
        type,
        options: {compression: Compression.UNCOMPRESSED, dictionary: false, ...options},
    }
}

// let type: ParquetType<string> = {
//     validate: (v) => v as string,
//     serialize: () => 'a',
// }

// let a = new ParquetTable('aaa', {
//     a: Column(type),
//     b: Column(type, {nullable: true}),
// })

// // type B = typeof a extends Table<infer R> ? R : never

// type A = TableRecord<typeof a>
