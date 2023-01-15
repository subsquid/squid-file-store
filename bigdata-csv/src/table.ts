import {toSnakeCase} from '@subsquid/util-naming'
import assert from 'assert'
import {TableSchema, Table, TableRecord, TableBuilder, Column, ColumnData, ColumnOptions} from '@subsquid/bigdata-table'
import {Dialect, dialects} from './dialect'
import {CsvType} from './types'

export interface TableOptions {
    extension?: string
    dialect?: Dialect
    header?: boolean
}

export type CsvColumnData<T extends CsvType<any> = CsvType<any>, O extends ColumnOptions = ColumnOptions> = ColumnData<
    T,
    O
>

export type CsvTableSchema = TableSchema<CsvColumnData>

export class CsvTable<T extends CsvTableSchema> extends Table<T> {
    private options: Required<TableOptions>
    constructor(readonly name: string, protected schema: T, options?: TableOptions) {
        super(name, schema)
        this.options = {extension: 'csv', header: true, dialect: dialects.excel, ...options}
    }

    createTableBuilder(): CsvTableBuilder<T> {
        return new CsvTableBuilder(this.columns, this.options)
    }

    getFileExtension() {
        return this.options.extension
    }
}

class CsvTableBuilder<T extends CsvTableSchema> implements TableBuilder<T> {
    private records: string[] = []
    private _size = 0

    constructor(private columns: Column<CsvColumnData>[], private options: Required<TableOptions>) {
        if (this.options.header) {
            let header = new Array<string>(this.columns.length)
            for (let i = 0; i < this.columns.length; i++) {
                let column = this.columns[i]
                let normalizedName = toSnakeCase(column.name)
                header[i] = this.hasSpecialChar(normalizedName, this.options.dialect)
                    ? `'${normalizedName}'`
                    : normalizedName
            }
            this.records.push(header.join(this.options.dialect.delimiter) + '\n')
        }
    }

    get size() {
        return this._size
    }

    toTable() {
        return this.records.join('')
    }

    append(records: TableRecord<T> | TableRecord<T>[]): TableBuilder<T> {
        records = Array.isArray(records) ? records : [records]

        for (let record of records) {
            let serializedValues: string[] = []
            for (let column of this.columns) {
                let value = record[column.name]
                if (value == null) {
                    assert(
                        value != null || column.data.options.nullable,
                        `Null value in non-nullable column "${column.name}"`
                    )
                } else {
                    column.data.type.validate(value)
                }
                let serializedValue = value == null ? `` : column.data.type.serialize(value)
                serializedValues.push(
                    this.hasSpecialChar(serializedValue, this.options.dialect)
                        ? `'${serializedValue}'`
                        : serializedValue
                )
            }
            let serializedRecord = serializedValues.join(this.options.dialect.delimiter) + '\n'
            this.records.push(serializedRecord)
            this._size += Buffer.byteLength(serializedRecord)
        }

        return this
    }

    private hasSpecialChar(str: string, dialect: Dialect) {
        return str.includes(dialect.delimiter) || str.includes('\n') || str.includes(dialect.quoteChar)
    }
}

export function Column<T extends CsvType<any>>(type: T): CsvColumnData<T>
export function Column<T extends CsvType<any>, O extends ColumnOptions>(type: T, options?: O): CsvColumnData<T, O>
export function Column<T extends CsvType<any>>(type: T, options?: ColumnOptions) {
    return {
        type,
        options: options || {},
    }
}

// let type: CsvType<string> = {
//     validate: (v) => v as string,
//     serialize: () => 'a',
// }

// let a = new CsvTable('aaa', {
//     a: Column(type),
//     b: Column(type, {nullable: true}),
// })

// // type B = typeof a extends Table<infer R> ? R : never

// type A = TableRecord<typeof a>
