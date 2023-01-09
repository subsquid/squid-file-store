import {assertNotNull} from '@subsquid/util-internal'
import {toSnakeCase} from '@subsquid/util-naming'
import assert from 'assert'
import {Table, TableHeader, TableRecord} from './table'
import {Dialect} from './util/dialect'

export class Chunk {
    private tableBuilders: Map<string, TableBuilder<any>>

    constructor(public from: number, public to: number, tables: Table<any>[], options: TableBuilderOptions) {
        this.tableBuilders = new Map(tables.map((t) => [t.name, new TableBuilder(t, options)]))
    }

    getTableBuilder<T extends TableHeader>(name: string): TableBuilder<T> {
        return assertNotNull(this.tableBuilders.get(name), `Table "${name}" does not exist`)
    }

    get size() {
        let total = 0
        for (let table of this.tableBuilders.values()) {
            total += table.size
        }
        return total
    }
}

export interface TableBuilderOptions {
    dialect: Dialect
    header: boolean
}

export class TableBuilder<T extends TableHeader> {
    private records: string[] = []
    private _size = 0

    constructor(private table: Table<T>, private options: TableBuilderOptions) {
        if (this.options.header) {
            let dialect = this.options.dialect
            let serializedHeader = this.table.fields.map((f) => toSnakeCase(f.name)).join(dialect.delimiter) + '\n'
            this.records.push(serializedHeader)
        }
    }

    get size() {
        return this._size
    }

    get data() {
        return this.records.join('')
    }

    append(records: TableRecord<T> | TableRecord<T>[]): void {
        records = Array.isArray(records) ? records : [records]

        for (let record of records) {
            let values: string[] = []
            for (let field of this.table.fields) {
                let value = record[field.name]
                assert(value != null || field.data.nullable, `Null value in non-nullable field "${field.name}"`)
                let serializedValue = value == null ? `` : field.data.type.serialize(value)
                values.push(this.hasSpecialChar(serializedValue) ? `'${serializedValue}'` : serializedValue)
            }
            let serializedRecord = values.join(this.options.dialect.delimiter) + '\n'
            this.records.push(serializedRecord)
            this._size += Buffer.byteLength(serializedRecord)
        }
    }

    private hasSpecialChar(str: string) {
        let dialect = this.options.dialect
        return str.includes(dialect.delimiter) || str.includes('\n') || str.includes(dialect.quoteChar)
    }
}
