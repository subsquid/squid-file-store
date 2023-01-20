import assert from 'assert'
import {
    Table as BaseTable,
    Column,
    ColumnData,
    ColumnOptions,
    ITableBuilder,
    TableRecord,
    TableSchema,
} from '@subsquid/bigdata-table'
import {toSnakeCase} from '@subsquid/util-naming'
import {Dialect, dialects, Quote} from './dialect'
import {CsvType} from './types'

export interface TableOptions {
    extension?: string
    dialect?: Dialect
    header?: boolean
}

type CsvColumnData<T extends CsvType<any> = CsvType<any>, O extends ColumnOptions = ColumnOptions> = ColumnData<T, O>

export class Table<T extends TableSchema<CsvColumnData>> extends BaseTable<T> {
    private options: Required<TableOptions>
    constructor(readonly name: string, protected schema: T, options?: TableOptions) {
        super(name, schema)
        this.options = {extension: 'csv', header: true, dialect: dialects.excel, ...options}
    }

    createTableBuilder(): TableBuilder<T> {
        return new TableBuilder(this.columns, this.options)
    }

    getFileExtension() {
        return this.options.extension
    }
}

class TableBuilder<T extends TableSchema<CsvColumnData>> implements ITableBuilder<T> {
    private records: string[] = []
    private _size = 0

    constructor(private columns: Column<CsvColumnData>[], private options: Required<TableOptions>) {
        if (this.options.header) {
            let header = new Array<string>(this.columns.length)
            for (let i = 0; i < this.columns.length; i++) {
                let column = this.columns[i]
                let normalizedName = toSnakeCase(column.name)
                header[i] = this.escape(normalizedName, false)
            }
            this.records.push(header.join(this.options.dialect.delimiter) + this.options.dialect.lineterminator)
        }
    }

    get size() {
        return this._size
    }

    flush() {
        let records = this.records
        this.records = []
        this._size = 0
        return records.join('')
    }

    append(records: TableRecord<T> | TableRecord<T>[]): TableBuilder<T> {
        records = Array.isArray(records) ? records : [records]

        for (let record of records) {
            let serializedValues: string[] = []
            for (let column of this.columns) {
                let value = record[column.name]
                if (value == null) {
                    assert(column.data.options.nullable, `Null value in non-nullable column "${column.name}"`)
                }
                let serializedValue = value == null ? `` : column.data.type.serialize(value)
                serializedValues.push(this.escape(serializedValue, column.data.type.isNumeric))
            }
            let serializedRecord =
                serializedValues.join(this.options.dialect.delimiter) + this.options.dialect.lineterminator
            this.records.push(serializedRecord)
            this._size += Buffer.byteLength(serializedRecord)
        }

        return this
    }

    private escape(str: string, isNumeric: boolean) {
        switch (this.options.dialect.quoting) {
            case Quote.NONE:
                return this.escapeAll(str)
            case Quote.MINIMAL:
                if (this.hasSpecialChar(str)) {
                    return this.quote(this.escapeQuote(str))
                } else {
                    return str
                }
            case Quote.NONNUMERIC:
                if (isNumeric) {
                    return this.escapeAll(str)
                } else {
                    return this.quote(this.escapeQuote(str))
                }
            case Quote.ALL: {
                return this.quote(this.escapeQuote(str))
            }
        }
    }

    private escapeQuote(str: string) {
        return this.escapeChars(str, [this.options.dialect.quoteChar])
    }

    private escapeAll(str: string) {
        let dialect = this.options.dialect
        return this.escapeChars(str, [
            dialect.delimiter,
            dialect.escapeChar || '',
            dialect.quoteChar,
            dialect.lineterminator,
        ])
    }

    private escapeChars(str: string, chars: string[]) {
        let quoteChar = this.options.dialect.quoteChar
        let escapeChar = this.options.dialect.escapeChar || quoteChar
        return str.replace(new RegExp(`[${chars.map((c) => `\(${c}\)`).join('')}]`, 'g'), (s) => `${escapeChar}${s}`)
    }

    private quote(str: string) {
        let quoteChar = this.options.dialect.quoteChar
        return quoteChar + str + quoteChar
    }

    private hasSpecialChar(str: string) {
        return (
            str.includes(this.options.dialect.delimiter) ||
            str.includes(this.options.dialect.lineterminator) ||
            str.includes(this.options.dialect.quoteChar)
        )
    }
}

export function Column<T extends CsvType<any>>(type: T): ColumnData<T>
export function Column<T extends CsvType<any>, O extends ColumnOptions>(
    type: T,
    options?: O
): ColumnData<T, O & ColumnOptions>
export function Column<T extends CsvType<any>>(type: T, options?: ColumnOptions) {
    return {
        type,
        options: {nullable: false, ...options},
    }
}
