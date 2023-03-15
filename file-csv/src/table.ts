import assert from 'assert'
import {Table as ITable, TableWriter as ITableWriter} from '@subsquid/file-store'
import {toSnakeCase} from '@subsquid/util-naming'
import {Dialect, Quote, dialects} from './dialect'

/**
 * Interface for CSV column data types.
 */
export interface Type<T> {
    serialize(value: T): string
    isNumeric: boolean
}

export interface TableOptions {
    /**
     * CSV dialect to be used. Defines data formatting details such as
     * escaping, quoting etc. See the dialects object exported by this module
     * for presets or write your own (must implement the Dialect interface).
     *
     * @default dialects.excel
     */
    dialect?: Dialect

    /**
     * Should the Table add a CSV header to the file?
     *
     * @default true
     */
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

/**
 * Table interface implementation for writing CSV files.
 *
 * @see https://docs.subsquid.io/basics/store/file-store/csv-table/
 */
export class Table<S extends TableSchema> implements ITable<ColumnsToTypes<S>> {
    private columns: Column[] = []
    private options: Required<TableOptions>

    /**
     * Table interface implementation for writing CSV files.
     *
     * @see https://docs.subsquid.io/basics/store/file-store/csv-table/
     *
     * @param name - name of the CSV file in every dataset partition folder
     * @param schema - a mapping from CSV column names to columns (see example)
     * @param options - table options
     *
     * @example
     * ```
     * import {
     *     Table,
     *     Column,
     *     Types,
     *     dialect
     * } from '@subsquid/file-store-csv'
     *
     * let transfersTable = new Table(
     *     'transfers.tsv',
     *     {
     *         from: Column(Types.String()),
     *         to: Column(Types.String()),
     *         value: Column(Types.Integer())
     *     },
     *     {
     *         dialect: dialects.excelTab,
     *         header: true
     *     }
     * )
     * ```
     */
    constructor(readonly name: string, schema: S, options?: TableOptions) {
        for (let column in schema) {
            this.columns.push({
                name: column,
                data: schema[column],
            })
        }
        this.options = {header: true, dialect: dialects.excel, ...options}
    }

    createWriter(): TableWriter<ColumnsToTypes<S>> {
        return new TableWriter(this.columns, this.options)
    }
}

class TableWriter<T extends Record<string, any>> implements ITableWriter<T> {
    private records: string[] = []
    private _size = 0

    private header: string | undefined

    constructor(private columns: Column[], private options: Required<TableOptions>) {
        if (this.options.header) {
            let columnNames: string[] = []
            for (let column of this.columns) {
                let normalizedName = toSnakeCase(column.name)
                columnNames.push(this.escape(normalizedName, false))
            }
            this.header = columnNames.join(this.options.dialect.delimiter) + this.options.dialect.lineterminator
        }
    }

    get size() {
        return this._size
    }

    flush(): Uint8Array {
        let table: string[] = []
        if (this.header) {
            table.push(this.header)
        }
        table = table.concat(this.records)

        this.reset()

        return Buffer.from(table.join(''), 'utf-8')
    }

    reset() {
        this.records = []
        this._size = 0
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

/**
 * Factory function for CSV columns
 *
 * @param type - a column data type
 * @param options - column options, interface: { nullable?: boolean }
 */
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
