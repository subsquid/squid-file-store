import assert from 'assert'
import {TableSchema, Table, TableRecord, TableBuilder, Column, ColumnData, ColumnOptions} from '@subsquid/bigdata-table'
import {ParquetType} from './types'
import {Builder, Data, makeBuilder, Table as ArrowTable, tableToIPC} from 'apache-arrow'
import {readParquet, writeParquet, Compression, WriterPropertiesBuilder} from 'parquet-wasm/node/arrow1'

export interface TableOptions {}

export type ParquetColumnData<
    T extends ParquetType<any> = ParquetType<any>,
    O extends ColumnOptions = ColumnOptions
> = ColumnData<T, O>

export type ParquetTableSchema = TableSchema<ParquetColumnData>

export class ParquetTable<T extends ParquetTableSchema> extends Table<T> {
    private options: Required<TableOptions>
    constructor(readonly name: string, protected schema: T, options?: TableOptions) {
        super(name, schema)
        this.options = {}
    }

    createTableBuilder(): ParquetTableBuilder<T> {
        return new ParquetTableBuilder(this.columns, this.options)
    }

    getFileExtension() {
        return 'parquet'
    }
}

class ParquetTableBuilder<T extends ParquetTableSchema> implements TableBuilder<T> {
    private columnBuilders: Record<string, Builder> = {}

    constructor(private columns: Column<ParquetColumnData>[], private options: Required<TableOptions>) {
        for (let column of columns) {
            this.columnBuilders[column.name] = makeBuilder({type: column.data.type.getArrowDataType()})
        }
    }

    get size() {
        let size = 0
        for (let column in this.columnBuilders) {
            size += this.columnBuilders[column].byteLength
        }
        return size
    }

    toTable() {
        let columnsData: Record<string, Data> = {}
        for (let column of this.columns) {
            columnsData[column.name] = this.columnBuilders[column.name].flush()
        }
        let arrowTable = new ArrowTable(columnsData)
        return writeParquet(tableToIPC(arrowTable), new WriterPropertiesBuilder().setCompression(0).build())
    }

    append(records: TableRecord<T> | TableRecord<T>[]): TableBuilder<T> {
        records = Array.isArray(records) ? records : [records]

        for (let column of this.columns) {
            let columnBuilder = this.columnBuilders[column.name]
            for (let record of records) {
                let value = record[column.name]
                if (value == null) {
                    assert(
                        value != null || column.data.options.nullable,
                        `Null value in non-nullable column "${column.name}"`
                    )
                } else {
                    column.data.type.validate(value)
                }
                columnBuilder.append(value)
            }
        }

        return this
    }
}

export function Column<T extends ParquetType<any>>(type: T): ParquetColumnData<T>
export function Column<T extends ParquetType<any>, O extends ColumnOptions>(
    type: T,
    options?: O
): ParquetColumnData<T, O>
export function Column<T extends ParquetType<any>>(type: T, options?: ColumnOptions) {
    return {
        type,
        options: options || {},
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
