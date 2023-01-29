import assert from 'assert'
import {Table as ITable, TableWriter as ITableWriter} from '@subsquid/file-store'
import {toSnakeCase} from '@subsquid/util-naming'
import {Dialect, Quote, dialects} from './dialect'
import {Type} from './types'

export interface TableOptions {
    extension?: string
    dialect?: Dialect
    header?: boolean
}

export interface ColumnOptions {
    nullable?: boolean
}

export interface ColumnData<
    T extends Type<any> = Type<any>,
    O extends Required<ColumnOptions> = Required<ColumnOptions>
> {
    type: T
    options: O
}

export interface Column {
    name: string
    data: ColumnData
}

export interface TableSchema {
    [column: string]: ColumnData
}

type NullableColumns<T extends Record<string, ColumnData>> = {
    [F in keyof T]: T[F] extends ColumnData<any, infer R> ? (R extends {nullable: true} ? F : never) : never
}[keyof T]

type ColumnsToTypes<T extends Record<string, ColumnData>> = {
    [F in Exclude<keyof T, NullableColumns<T>>]: T[F] extends ColumnData<Type<infer R>> ? R : never
} & {
    [F in Extract<keyof T, NullableColumns<T>>]?: T[F] extends ColumnData<Type<infer R>> ? R | null | undefined : never
}

export class Table<S extends TableSchema> implements ITable<ColumnsToTypes<S>> {
    private columns: Column[] = []
    private options: Required<TableOptions>
    constructor(readonly name: string, schema: S, options?: TableOptions) {
        for (let column in schema) {
            this.columns.push({
                name: column,
                data: schema[column],
            })
        }
        this.options = {extension: 'csv', header: true, dialect: dialects.excel, ...options}
    }

    createWriter(): TableWriter<ColumnsToTypes<S>> {
        return new TableWriter(this.columns, this.options)
    }
}

class TableWriter<T extends Record<string, any>> implements ITableWriter<T> {
    private records: string[] = []
    private _size = 0

    constructor(private columns: Column[], private options: Required<TableOptions>) {
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

    flush(): Uint8Array {
        let records = this.records
        this.records = []
        this._size = 0
        return Buffer.from(records.join(''), 'utf-8')
    }

    write(record: T): this {
        return this.writeMany([record])
    }

    writeMany(records: T[]): this {
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

export function Column<T extends Type<any>>(type: T): ColumnData<T>
export function Column<T extends Type<any>, O extends ColumnOptions>(
    type: T,
    options?: O
): ColumnData<T, O & Required<ColumnOptions>>
export function Column<T extends Type<any>>(type: T, options?: ColumnOptions) {
    return {
        type,
        options: {nullable: false, ...options},
    }
}
